package io.tapdata.mongodb.data;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
/**
 * A BasicDBObject that abandon empty key and null value.
 * provide some useful method
 */
public class CleanDocument extends Document {

	private static final long serialVersionUID = 8027097799463248640L;
	CleanDocument() {}
	CleanDocument(Document dbo) {
	    if (dbo != null) {
	        putAll(dbo);
	    }
	}

	@Override
	public Object put(String key, Object val) {
		if(val == null || (StringUtils.isBlank(key)))
			return null;

		return super.put(key, val);
	}
	
	@Override
	public CleanDocument append(String key, Object val) {
	    this.put(key, val);
	    return this;
	}
	
    /**
     * @param key
     * @param clazz Only support DBObjectable
     * @return clazz instance
     */
    @SuppressWarnings("unchecked")
	public <V> V get(String key, Class<? super V> clazz) {
        Object value = super.get(key);
        if (value == null) {
            return null;
        } else {
        	return (V) value;
        }
    }
    
    /** Returns the value of a field as an <code>integer</code>.
     * @param key the field to look for
     * @return the field value or null
     */
	public Integer getInteger( String key ){
        Object o = get(key);
        if ( o == null )
            return null;

        return ((Number)o).intValue();
    }

    /** Returns the value of a field as an <code>Integer</code>.
     * @param key the field to look for
     * @return the field value (or default)
     */
	public Integer getInteger( String key , Integer defaultValue ){
        Object foo = get( key );
        if ( foo == null )
            return defaultValue;

        return ((Number)foo).intValue();
    }

    /**
     * Returns the value of a field as a <code>Long</code>.
     *
     * @param key the field to return
     * @return the field value or null
     */
	public Long getLongObject( String key){
        Object foo = get( key );
        if (foo == null) 
            return null;
        return ((Number)foo).longValue();
    }

    /**
     * Returns the value of a field as an <code>Long</code>.
     * @param key the field to look for
     * @return the field value (or default)
     */
	public Long getLongObject( String key , Long defaultValue ) {
        Object foo = get( key );
        if ( foo == null )
            return defaultValue;

        return ((Number)foo).longValue();
    }

    /**
     * Returns the value of a field as a <code>Double</code>.
     *
     * @param key the field to return
     * @return the field value or null
     */
	public Double getDoubleObject( String key){
        Object foo = get( key );
        if (foo == null) 
            return null;
        return ((Number)foo).doubleValue();
    }

    /**
     * Returns the value of a field as an <code>Double</code>.
     * @param key the field to look for
     * @return the field value (or default)
     */
	public Double getDoubleObject( String key , Double defaultValue ) {
        Object foo = get( key );
        if ( foo == null )
            return defaultValue;

        return ((Number)foo).doubleValue();
    }
    
}
