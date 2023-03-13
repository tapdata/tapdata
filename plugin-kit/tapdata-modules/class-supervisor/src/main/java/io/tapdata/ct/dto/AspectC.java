package io.tapdata.ct.dto;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class AspectC {

    @Pointcut("execution(* java.lang.Thread.start())")
    public Object function(){
        System.out.println("Gavin--------");
        return null;
    }
}
