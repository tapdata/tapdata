/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.jodah.typetools;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import sun.misc.Unsafe;

/**
 * Enhanced type resolution utilities.
 *
 * @author Jonathan Halterman
 */
@SuppressWarnings("restriction")
public final class TypeResolver {
  /** Cache of type variable/argument pairs */
  private static final Map<Class<?>, Reference<Map<TypeVariable<?>, Type>>> TYPE_VARIABLE_CACHE = Collections
      .synchronizedMap(new WeakHashMap<Class<?>, Reference<Map<TypeVariable<?>, Type>>>());
  private static volatile boolean CACHE_ENABLED = true;
  private static boolean RESOLVES_LAMBDAS;
  private static Object JAVA_LANG_ACCESS;
  private static Method GET_CONSTANT_POOL;
  private static Method GET_CONSTANT_POOL_SIZE;
  private static Method GET_CONSTANT_POOL_METHOD_AT;
  private static final Map<String, Method> OBJECT_METHODS = new HashMap<String, Method>();
  private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPERS;
  private static final Double JAVA_VERSION;

  static {
    JAVA_VERSION = Double.parseDouble(System.getProperty("java.specification.version", "0"));

    try {
      final Unsafe unsafe = AccessController.doPrivileged(new PrivilegedExceptionAction<Unsafe>() {
        @Override
        public Unsafe run() throws Exception {
          final Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);

          return (Unsafe) f.get(null);
        }
      });

      Class<?> sharedSecretsClass;
      AccessMaker accessSetter;
      if (JAVA_VERSION < 9) {
        sharedSecretsClass = Class.forName("sun.misc.SharedSecrets");
        // Java 8 and lower can simply call setAccessible
        accessSetter = new AccessMaker() {
          @Override
          public void makeAccessible(AccessibleObject accessibleObject) {
            accessibleObject.setAccessible(true);
          }
        };
      } else if (JAVA_VERSION < 12) {
          try {
            sharedSecretsClass = Class.forName("jdk.internal.misc.SharedSecrets");
          } catch (ClassNotFoundException e) {
            // In Oracle JDK 11.0.6, SharedSecrets was moved from jdk.internal.misc to jdk.internal.access.
            sharedSecretsClass = Class.forName("jdk.internal.access.SharedSecrets");
          }
          // access control got strengthed in Java 9, but can be circumvented with Unsafe.
          Field overrideField = AccessibleObject.class.getDeclaredField("override");
          final long overrideFieldOffset = unsafe.objectFieldOffset(overrideField);
          accessSetter = new AccessMaker() {
            @Override
            public void makeAccessible(AccessibleObject accessibleObject) {
              unsafe.putBoolean(accessibleObject, overrideFieldOffset, true);
            }
        };
      } else {
          sharedSecretsClass = Class.forName("jdk.internal.access.SharedSecrets");
          // In Java 12, AccessibleObject.override was added to the reflection blacklist.
          // Access checking can still be circumvented by using the Unsafe technique to get the implementation lookup from MethodHandles.
          Field implLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
          long implLookupFieldOffset = unsafe.staticFieldOffset(implLookupField);
          Object lookupStaticFieldBase = unsafe.staticFieldBase(implLookupField);
          MethodHandles.Lookup implLookup = (MethodHandles.Lookup) unsafe.getObject(lookupStaticFieldBase, implLookupFieldOffset);
          final MethodHandle overrideSetter = implLookup.findSetter(AccessibleObject.class, "override", boolean.class);
          accessSetter = new AccessMaker() {
            @Override
            public void makeAccessible(AccessibleObject object) throws Throwable {
              overrideSetter.invokeWithArguments(new Object[] {object, true});
            }
        };
      }
      Method javaLangAccessGetter = sharedSecretsClass.getMethod("getJavaLangAccess");
      accessSetter.makeAccessible(javaLangAccessGetter);
      JAVA_LANG_ACCESS = javaLangAccessGetter.invoke(null);
      GET_CONSTANT_POOL = JAVA_LANG_ACCESS.getClass().getMethod("getConstantPool", Class.class);

      String constantPoolName = JAVA_VERSION < 9 ? "sun.reflect.ConstantPool" : "jdk.internal.reflect.ConstantPool";
      Class<?> constantPoolClass = Class.forName(constantPoolName);
      GET_CONSTANT_POOL_SIZE = constantPoolClass.getDeclaredMethod("getSize");
      GET_CONSTANT_POOL_METHOD_AT = constantPoolClass.getDeclaredMethod("getMethodAt", int.class);

      // setting the methods as accessible
      accessSetter.makeAccessible(GET_CONSTANT_POOL);
      accessSetter.makeAccessible(GET_CONSTANT_POOL_SIZE);
      accessSetter.makeAccessible(GET_CONSTANT_POOL_METHOD_AT);

      // additional checks - make sure we get a result when invoking the Class::getConstantPool and
      // ConstantPool::getSize on a class
      Object constantPool = GET_CONSTANT_POOL.invoke(JAVA_LANG_ACCESS, Object.class);
      GET_CONSTANT_POOL_SIZE.invoke(constantPool);

      for (Method method : Object.class.getDeclaredMethods())
        OBJECT_METHODS.put(method.getName(), method);

      RESOLVES_LAMBDAS = true;
    } catch (Throwable ignore) {
    }

    Map<Class<?>, Class<?>> types = new HashMap<Class<?>, Class<?>>();
    types.put(boolean.class, Boolean.class);
    types.put(byte.class, Byte.class);
    types.put(char.class, Character.class);
    types.put(double.class, Double.class);
    types.put(float.class, Float.class);
    types.put(int.class, Integer.class);
    types.put(long.class, Long.class);
    types.put(short.class, Short.class);
    types.put(void.class, Void.class);
    PRIMITIVE_WRAPPERS = Collections.unmodifiableMap(types);
  }
  
  private interface AccessMaker {
    void makeAccessible(AccessibleObject object) throws Throwable;
  }
  
  /** An unknown type. */
  public static final class Unknown {
    private Unknown() {
    }
  }

  private TypeResolver() {
  }

  /**
   * Enables the internal caching of resolved TypeVariables.
   */
  public static void enableCache() {
    CACHE_ENABLED = true;
  }

  /**
   * Disables the internal caching of resolved TypeVariables.
   */
  public static void disableCache() {
    TYPE_VARIABLE_CACHE.clear();
    CACHE_ENABLED = false;
  }

  /**
   * Returns the raw class representing the argument for the {@code type} using type variable information from the
   * {@code subType}. If no arguments can be resolved then {@code Unknown.class} is returned.
   *
   * @param type to resolve argument for
   * @param subType to extract type variable information from
   * @return argument for {@code type} else {@link Unknown}.class if no type arguments are declared
   * @throws IllegalArgumentException if more or less than one argument is resolved for the {@code type}
   */
  public static <T, S extends T> Class<?> resolveRawArgument(Class<T> type, Class<S> subType) {
    return resolveRawArgument(resolveGenericType(type, subType), subType);
  }

  /**
   * Returns the raw class representing the argument for the {@code genericType} using type variable information from
   * the {@code subType}. If {@code genericType} is an instance of class, then {@code genericType} is returned. If no
   * arguments can be resolved then {@code Unknown.class} is returned.
   *
   * @param genericType to resolve argument for
   * @param subType to extract type variable information from
   * @return argument for {@code genericType} else {@link Unknown}.class if no type arguments are declared
   * @throws IllegalArgumentException if more or less than one argument is resolved for the {@code genericType}
   */
  public static Class<?> resolveRawArgument(Type genericType, Class<?> subType) {
    Class<?>[] arguments = resolveRawArguments(genericType, subType);
    if (arguments == null)
      return Unknown.class;

    if (arguments.length != 1)
      throw new IllegalArgumentException(
          "Expected 1 argument for generic type " + genericType + " but found " + arguments.length);

    return arguments[0];
  }

  /**
   * Returns an array of raw classes representing arguments for the {@code type} using type variable information from
   * the {@code subType}. Arguments for {@code type} that cannot be resolved are returned as {@code Unknown.class}. If
   * no arguments can be resolved then {@code null} is returned.
   *
   * @param type to resolve arguments for
   * @param subType to extract type variable information from
   * @return array of raw classes representing arguments for the {@code type} else {@code null} if no type arguments are
   *         declared
   */
  public static <T, S extends T> Class<?>[] resolveRawArguments(Class<T> type, Class<S> subType) {
    return resolveRawArguments(resolveGenericType(type, subType), subType);
  }

  /**
   * Traverses a generic type and replaces all type variables and wildcard types with concrete types (if possible),
   * by using the type information from given {@code context}.
   * A convenience method which largely works the same as {@link #reify(Type, Class)}, but first resolves the
   * generic type of {@code type}.
   *
   * @param type the class whose generic type to traverse
   * @param context the class that serves as starting point to resolve replacements of type variables
   * @return a type that is structurally the same as {@code type}, except that type variables and wildcard types
   *         have been replaced with concrete types
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is not an instance of one of
   *         the following types: {@link Class}, {@link TypeVariable}, {@link WildcardType}, {@link ParameterizedType},
   *         {@link GenericArrayType}.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link WildcardType} that
   *         does not have exactly one upper bound, or does not have no lower bounds.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link GenericArrayType}
   *         whose generic component type cannot be reified to an instance of {@link Class}.
   */
  public static <T, S extends T> Type reify(Class<T> type, Class<S> context) {
    return reify(resolveGenericType(type, context), getTypeVariableMap(context, null));
  }

  /**
   * Traverses a generic type and replaces all type variables and wildcard types with concrete types (if possible),
   * by using the type information from given {@code context}.
   *
   * Generic types used as input to this method are commonly obtained using reflection, e.g. via
   * {@link Field#getGenericType()}, {@link Method#getGenericReturnType()}, {@link Method#getGenericParameterTypes()}.
   *
   * Example:
   * <blockquote><pre>{@code
   *   class A<T> {
   *     public T something;
   *     public Optional<List<T>> compute() { ... }
   *   }
   *
   *   class B extends A<Number> {
   *     public <? extends Collection<List<?>>> collect() { ... }
   *   }
   * }</pre></blockquote>
   *
   * Reifying the generic return type of the method {@code compute} with {@code B.class} as {@code context} will
   * yield the parameterized type {@code Optional<List<Number>>}. Note that not the raw type ({@link java.util.Optional}
   * is returned, but the input type is reified recursively.
   * Reifying the generic type of the field {@code something} with {@code B.class} as {@code context} will yield
   * {@code Number.class}.
   *
   * Note that type variables with no explicit upper bound are reified to {@link Object}, and {@code Unknown.class} is
   * never returned.
   *
   * @param type the generic type to traverse
   * @param context the class that serves as starting point to resolve replacements of type variables
   * @return a type that is structurally the same as {@code type}, except that type variables and wildcard types
   *         have been replaced with concrete types
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is not an instance of one of
   *         the following types: {@link Class}, {@link TypeVariable}, {@link WildcardType}, {@link ParameterizedType},
   *         {@link GenericArrayType}.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link WildcardType} that
   *         does not have exactly one upper bound, or does not have no lower bounds.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link GenericArrayType}
   *         whose generic component type cannot be reified to an instance of {@link Class}.
   */
  public static Type reify(Type type, Class<?> context) {
    return reify(type, getTypeVariableMap(context, null));
  }

  /**
   * Traverses a generic type and replaces all type variables and wildcard types with concrete types (if possible).
   * A convenience wrapper around {@link #reify(Type, Class)}, for when no context is needed/available.
   *
   * Generic types used as input to this method are commonly obtained using reflection, e.g. via
   * {@link Field#getGenericType()}, {@link Method#getGenericReturnType()}, {@link Method#getGenericParameterTypes()}.
   *
   * Example:
   * <blockquote><pre>{@code
   *   class X {
   *     public List<? extends Collection<List<? extends Number>>> collectList() { ... }
   *     public Set<?> collectSet() { ... }
   *   }
   * }</pre></blockquote>
   *
   * Reifying the generic return type of the method {@code collectList} will yield the parameterized type
   * {@code List<Collection<List<Number>>>}.
   * Reifying the generic return type of the method {@code collectSet} will yield the parameterized type
   * {@code Set<Object>}, since there is no explicit upper bound for the wildcard type given.
   *
   * @param type the generic type to traverse
   * @return a type that is structurally the same as {@code type}, except that type variables and wildcard types
   *         have been replaced with concrete types
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is not an instance of one of
   *         the following types: {@link Class}, {@link TypeVariable}, {@link WildcardType}, {@link ParameterizedType},
   *         {@link GenericArrayType}.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link WildcardType} that
   *         does not have exactly one upper bound, or does not have no lower bounds.
   * @throws UnsupportedOperationException if {@code type} (or a type that it references) is a {@link GenericArrayType}
   *         whose generic component type cannot be reified to an instance of {@link Class}.
   */
  public static Type reify(Type type) {
    return reify(type, new HashMap<TypeVariable<?>, Type>(0));
  }

  /**
   * Returns an array of raw classes representing arguments for the {@code genericType} using type variable information
   * from the {@code subType}. Arguments for {@code genericType} that cannot be resolved are returned as
   * {@code Unknown.class}. If no arguments can be resolved then {@code null} is returned.
   *
   * @param genericType to resolve arguments for
   * @param subType to extract type variable information from
   * @return array of raw classes representing arguments for the {@code genericType} else {@code null} if no type
   *         arguments are declared
   */
  public static Class<?>[] resolveRawArguments(Type genericType, Class<?> subType) {
    Class<?>[] result = null;
    Class<?> functionalInterface = null;

    // Handle lambdas
    if (RESOLVES_LAMBDAS && subType.isSynthetic()) {
      Class<?> fi = genericType instanceof ParameterizedType
          && ((ParameterizedType) genericType).getRawType() instanceof Class
              ? (Class<?>) ((ParameterizedType) genericType).getRawType()
              : genericType instanceof Class ? (Class<?>) genericType : null;
      if (fi != null && fi.isInterface())
        functionalInterface = fi;
    }

    if (genericType instanceof ParameterizedType) {
      ParameterizedType paramType = (ParameterizedType) genericType;
      Type[] arguments = paramType.getActualTypeArguments();
      result = new Class[arguments.length];
      for (int i = 0; i < arguments.length; i++)
        result[i] = resolveRawClass(arguments[i], subType, functionalInterface);
    } else if (genericType instanceof TypeVariable) {
      result = new Class[1];
      result[0] = resolveRawClass(genericType, subType, functionalInterface);
    } else if (genericType instanceof Class) {
      TypeVariable<?>[] typeParams = ((Class<?>) genericType).getTypeParameters();
      result = new Class[typeParams.length];
      for (int i = 0; i < typeParams.length; i++)
        result[i] = resolveRawClass(typeParams[i], subType, functionalInterface);
    }

    return result;
  }

  /**
   * Returns the generic {@code type} using type variable information from the {@code subType} else {@code null} if the
   * generic type cannot be resolved.
   *
   * @param type to resolve generic type for
   * @param subType to extract type variable information from
   * @return generic {@code type} else {@code null} if it cannot be resolved
   */
  public static Type resolveGenericType(Class<?> type, Type subType) {
    Class<?> rawType;
    if (subType instanceof ParameterizedType)
      rawType = (Class<?>) ((ParameterizedType) subType).getRawType();
    else
      rawType = (Class<?>) subType;

    if (type.equals(rawType))
      return subType;

    Type result;
    if (type.isInterface()) {
      for (Type superInterface : rawType.getGenericInterfaces())
        if (superInterface != null && !superInterface.equals(Object.class))
          if ((result = resolveGenericType(type, superInterface)) != null)
            return result;
    }

    Type superClass = rawType.getGenericSuperclass();
    if (superClass != null && !superClass.equals(Object.class))
      if ((result = resolveGenericType(type, superClass)) != null)
        return result;

    return null;
  }

  /**
   * Resolves the raw class for the {@code genericType}, using the type variable information from the {@code subType}
   * else {@link Unknown} if the raw class cannot be resolved.
   *
   * @param genericType to resolve raw class for
   * @param subType to extract type variable information from
   * @return raw class for the {@code genericType} else {@link Unknown} if it cannot be resolved
   */
  public static Class<?> resolveRawClass(Type genericType, Class<?> subType) {
    return resolveRawClass(genericType, subType, null);
  }

  private static Class<?> resolveRawClass(Type genericType, Class<?> subType, Class<?> functionalInterface) {
    if (genericType instanceof Class) {
      return (Class<?>) genericType;
    } else if (genericType instanceof ParameterizedType) {
      return resolveRawClass(((ParameterizedType) genericType).getRawType(), subType, functionalInterface);
    } else if (genericType instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) genericType;
      Class<?> component = resolveRawClass(arrayType.getGenericComponentType(), subType, functionalInterface);
      return Array.newInstance(component, 0).getClass();
    } else if (genericType instanceof TypeVariable) {
      TypeVariable<?> variable = (TypeVariable<?>) genericType;
      genericType = getTypeVariableMap(subType, functionalInterface).get(variable);
      genericType = genericType == null ? resolveBound(variable)
          : resolveRawClass(genericType, subType, functionalInterface);
    }

    return genericType instanceof Class ? (Class<?>) genericType : Unknown.class;
  }

  private static Type reify(final Type genericType, final Map<TypeVariable<?>, Type> typeVariableTypeMap) {
    // Check for terminal cases to avoid allocating HashMap and trivial call.
    if (genericType == null)
      return null;
    else if (genericType instanceof Class<?>)
      return genericType;
    else
      return reify(genericType, typeVariableTypeMap, new HashMap<ParameterizedType, ReifiedParameterizedType>());
  }

  /**
   * Works like {@link #resolveRawClass(Type, Class, Class)} but does not stop at raw classes. Instead, traverses
   * referenced types.
   *
   * @param partial contains a mapping of generic types to reified types. A value of {@code null} inside a
   *        {@link ReifiedParameterizedType} instance means that this type is currently being reified.
   */
  private static Type reify(Type genericType, final Map<TypeVariable<?>, Type> typeVariableMap, Map<ParameterizedType, ReifiedParameterizedType> partial) {
    // Terminal case.
    if (genericType instanceof Class<?>)
      return genericType;

    // Recursive cases.
    if (genericType instanceof ParameterizedType) {
      final ParameterizedType parameterizedType = (ParameterizedType) genericType;
      // Self-referential type needs special attention. Otherwise we might accidentally overflow the stack.
      if (partial.containsKey(parameterizedType)) {
        ReifiedParameterizedType res = partial.get(genericType);
        res.addReifiedTypeArgument(res);
        return res;
      }
      final Type[] genericTypeArguments =  parameterizedType.getActualTypeArguments();
      final ReifiedParameterizedType result = new ReifiedParameterizedType(parameterizedType);
      partial.put(parameterizedType, result);
      for (Type genericTypeArgument : genericTypeArguments) {
        Type reified = reify(genericTypeArgument, typeVariableMap, partial);
        // Self-references are added as soon as they are detected, see above.
        // In this case, skip adding.
        if (reified != result) {
          result.addReifiedTypeArgument(reified);
        }
      }
      return result;
    } else if (genericType instanceof GenericArrayType) {
      final GenericArrayType genericArrayType = (GenericArrayType) genericType;
      final Type genericComponentType = genericArrayType.getGenericComponentType();
      final Type reifiedComponentType = reify(genericArrayType.getGenericComponentType(), typeVariableMap, partial);

      if (genericComponentType == reifiedComponentType)
        return genericComponentType;

      if (reifiedComponentType instanceof Class<?>)
        return Array.newInstance((Class<?>) reifiedComponentType, 0).getClass();

      throw new UnsupportedOperationException(
          "Attempted to reify generic array type, whose generic component type " +
          "could not be reified to some Class<?>. Handling for this case is not implemented");
    } else if (genericType instanceof TypeVariable<?>) {
      final TypeVariable<?> typeVariable = (TypeVariable<?>) genericType;
      final Type mapping = typeVariableMap.get(typeVariable);
      if (mapping != null)
        return reify(mapping, typeVariableMap, partial);
      // NOTE: According to https://docs.oracle.com/javase/tutorial/java/generics/bounded.html
      // if there are multiple upper bounds where one bound is a class, then this must be the
      // leftmost/first bound. Therefore we blindly take this one, hoping it is the most relevant.
      // Hibernate does the same when erasing types, see also
      // https://github.com/hibernate/hibernate-validator/blob/6.0/engine/src/main/java/org/hibernate/validator/internal/util/TypeHelper.java#L181-L186
      return reify(typeVariable.getBounds()[0], typeVariableMap, partial);
    } else if (genericType instanceof WildcardType) {
      final WildcardType wildcardType = (WildcardType) genericType;
      final Type[] upperBounds = wildcardType.getUpperBounds();
      final Type[] lowerBounds = wildcardType.getLowerBounds();
      if (upperBounds.length == 1 && lowerBounds.length == 0)
        return reify(upperBounds[0], typeVariableMap, partial);

      throw new UnsupportedOperationException(
          "Attempted to reify wildcard type with name '" + wildcardType.getTypeName() +
          "' which has " + upperBounds.length + " upper bounds and " + lowerBounds.length +
          " lower bounds. Reification of wildcard types is only supported for" +
          " the trivial case of exactly 1 upper bound and 0 lower bounds.");
    }
    throw new UnsupportedOperationException(
        "Reification of type with name '" + genericType.getTypeName() + "' and " +
        "class name '" + genericType.getClass().getName() + "' is not implemented.");
  }

  private static Map<TypeVariable<?>, Type> getTypeVariableMap(final Class<?> targetType,
      Class<?> functionalInterface) {
    Reference<Map<TypeVariable<?>, Type>> ref = TYPE_VARIABLE_CACHE.get(targetType);
    Map<TypeVariable<?>, Type> map = ref != null ? ref.get() : null;

    if (map == null) {
      map = new HashMap<TypeVariable<?>, Type>();

      // Populate lambdas
      if (functionalInterface != null)
        populateLambdaArgs(functionalInterface, targetType, map);

      // Populate interfaces
      populateSuperTypeArgs(targetType.getGenericInterfaces(), map, functionalInterface != null);

      // Populate super classes and interfaces
      Type genericType = targetType.getGenericSuperclass();
      Class<?> type = targetType.getSuperclass();
      while (type != null && !Object.class.equals(type)) {
        if (genericType instanceof ParameterizedType)
          populateTypeArgs((ParameterizedType) genericType, map, false);
        populateSuperTypeArgs(type.getGenericInterfaces(), map, false);

        genericType = type.getGenericSuperclass();
        type = type.getSuperclass();
      }

      // Populate enclosing classes
      type = targetType;
      while (type.isMemberClass()) {
        genericType = type.getGenericSuperclass();
        if (genericType instanceof ParameterizedType)
          populateTypeArgs((ParameterizedType) genericType, map, functionalInterface != null);

        type = type.getEnclosingClass();
      }

      if (CACHE_ENABLED)
        TYPE_VARIABLE_CACHE.put(targetType, new WeakReference<Map<TypeVariable<?>, Type>>(map));
    }

    return map;
  }

  /**
   * Populates the {@code map} with with variable/argument pairs for the given {@code types}.
   */
  private static void populateSuperTypeArgs(final Type[] types, final Map<TypeVariable<?>, Type> map,
      boolean depthFirst) {
    for (Type type : types) {
      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        if (!depthFirst)
          populateTypeArgs(parameterizedType, map, depthFirst);
        Type rawType = parameterizedType.getRawType();
        if (rawType instanceof Class)
          populateSuperTypeArgs(((Class<?>) rawType).getGenericInterfaces(), map, depthFirst);
        if (depthFirst)
          populateTypeArgs(parameterizedType, map, depthFirst);
      } else if (type instanceof Class) {
        populateSuperTypeArgs(((Class<?>) type).getGenericInterfaces(), map, depthFirst);
      }
    }
  }

  /**
   * Populates the {@code map} with variable/argument pairs for the given {@code type}.
   */
  private static void populateTypeArgs(ParameterizedType type, Map<TypeVariable<?>, Type> map, boolean depthFirst) {
    if (type.getRawType() instanceof Class) {
      TypeVariable<?>[] typeVariables = ((Class<?>) type.getRawType()).getTypeParameters();
      Type[] typeArguments = type.getActualTypeArguments();

      if (type.getOwnerType() != null) {
        Type owner = type.getOwnerType();
        if (owner instanceof ParameterizedType)
          populateTypeArgs((ParameterizedType) owner, map, depthFirst);
      }

      for (int i = 0; i < typeArguments.length; i++) {
        TypeVariable<?> variable = typeVariables[i];
        Type typeArgument = typeArguments[i];

        if (typeArgument instanceof Class) {
          map.put(variable, typeArgument);
        } else if (typeArgument instanceof GenericArrayType) {
          map.put(variable, typeArgument);
        } else if (typeArgument instanceof ParameterizedType) {
          map.put(variable, typeArgument);
        } else if (typeArgument instanceof TypeVariable) {
          TypeVariable<?> typeVariableArgument = (TypeVariable<?>) typeArgument;
          if (depthFirst) {
            Type existingType = map.get(variable);
            if (existingType != null) {
              map.put(typeVariableArgument, existingType);
              continue;
            }
          }

          Type resolvedType = map.get(typeVariableArgument);
          if (resolvedType == null)
            resolvedType = resolveBound(typeVariableArgument);
          map.put(variable, resolvedType);
        }
      }
    }
  }

  /**
   * Resolves the first bound for the {@code typeVariable}, returning {@code Unknown.class} if none can be resolved.
   */
  public static Type resolveBound(TypeVariable<?> typeVariable) {
    Type[] bounds = typeVariable.getBounds();
    if (bounds.length == 0)
      return Unknown.class;

    Type bound = bounds[0];
    if (bound instanceof TypeVariable)
      bound = resolveBound((TypeVariable<?>) bound);

    return bound == Object.class ? Unknown.class : bound;
  }

  /**
   * Populates the {@code map} with variable/argument pairs for the {@code functionalInterface}.
   */
  private static void populateLambdaArgs(Class<?> functionalInterface, final Class<?> lambdaType,
      Map<TypeVariable<?>, Type> map) {
    if (RESOLVES_LAMBDAS) {
      // Find SAM
      for (Method m : functionalInterface.getMethods()) {
        if (!isDefaultMethod(m) && !Modifier.isStatic(m.getModifiers()) && !m.isBridge()) {
          // Skip methods that override Object.class
          Method objectMethod = OBJECT_METHODS.get(m.getName());
          if (objectMethod != null && Arrays.equals(m.getTypeParameters(), objectMethod.getTypeParameters()))
            continue;

          // Get functional interface's type params
          Type returnTypeVar = m.getGenericReturnType();
          Type[] paramTypeVars = m.getGenericParameterTypes();

          Member member = getMemberRef(lambdaType);
          if (member == null)
            return;

          // Populate return type argument
          if (returnTypeVar instanceof TypeVariable) {
            Class<?> returnType = member instanceof Method ? ((Method) member).getReturnType()
                : ((Constructor<?>) member).getDeclaringClass();
            returnType = wrapPrimitives(returnType);
            if (!returnType.equals(Void.class))
              map.put((TypeVariable<?>) returnTypeVar, returnType);
          }

          Class<?>[] arguments = member instanceof Method ? ((Method) member).getParameterTypes()
              : ((Constructor<?>) member).getParameterTypes();

          // Populate object type from arbitrary object method reference
          int paramOffset = 0;
          if (paramTypeVars.length > 0 && paramTypeVars[0] instanceof TypeVariable
              && paramTypeVars.length == arguments.length + 1) {
            Class<?> instanceType = member.getDeclaringClass();
            map.put((TypeVariable<?>) paramTypeVars[0], instanceType);
            paramOffset = 1;
          }

          // Handle additional arguments that are captured from the lambda's enclosing scope
          int argOffset = 0;
          if (paramTypeVars.length < arguments.length) {
            argOffset = arguments.length - paramTypeVars.length;
          }

          // Populate type arguments
          for (int i = 0; i + argOffset < arguments.length; i++) {
            if (paramTypeVars[i] instanceof TypeVariable)
              map.put((TypeVariable<?>) paramTypeVars[i + paramOffset], wrapPrimitives(arguments[i + argOffset]));
          }

          return;
        }
      }
    }
  }

  private static boolean isDefaultMethod(Method m) {
    return JAVA_VERSION >= 1.8 && m.isDefault();
  }

  private static Member getMemberRef(Class<?> type) {
    Object constantPool;
    try {
      constantPool = GET_CONSTANT_POOL.invoke(JAVA_LANG_ACCESS, type);
    } catch (Exception ignore) {
      return null;
    }

    Member result = null;
    for (int i = getConstantPoolSize(constantPool) - 1; i >= 0; i--) {
      Member member = getConstantPoolMethodAt(constantPool, i);
      // Skip SerializedLambda constructors and members of the "type" class
      if (member == null
          || (member instanceof Constructor
              && member.getDeclaringClass().getName().equals("java.lang.invoke.SerializedLambda"))
          || member.getDeclaringClass().isAssignableFrom(type))
        continue;

      result = member;

      // Return if not valueOf method
      if (!(member instanceof Method) || !isAutoBoxingMethod((Method) member))
        break;
    }

    return result;
  }

  private static boolean isAutoBoxingMethod(Method method) {
    Class<?>[] parameters = method.getParameterTypes();
    return method.getName().equals("valueOf") && parameters.length == 1 && parameters[0].isPrimitive()
        && wrapPrimitives(parameters[0]).equals(method.getDeclaringClass());
  }

  private static Class<?> wrapPrimitives(Class<?> clazz) {
    return clazz.isPrimitive() ? PRIMITIVE_WRAPPERS.get(clazz) : clazz;
  }

  private static int getConstantPoolSize(Object constantPool) {
    try {
      return (Integer) GET_CONSTANT_POOL_SIZE.invoke(constantPool);
    } catch (Exception ignore) {
      return 0;
    }
  }

  private static Member getConstantPoolMethodAt(Object constantPool, int i) {
    try {
      return (Member) GET_CONSTANT_POOL_METHOD_AT.invoke(constantPool, i);
    } catch (Exception ignore) {
      return null;
    }
  }
}
