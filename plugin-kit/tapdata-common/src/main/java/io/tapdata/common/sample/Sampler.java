package io.tapdata.common.sample;

/**
 * This is an interface which you can implement to add your owner sampler.
 *
 * What you have to do is to add sample calculation logic into the value function
 * or simply add a lambda function which can satisfy the {@link Sampler} interface.
 *
 * @author Dexter
 */
public interface Sampler {
    Number value();
}
