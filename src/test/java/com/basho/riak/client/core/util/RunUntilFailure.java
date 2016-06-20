package com.basho.riak.client.core.util;


import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class RunUntilFailure extends Runner
{
    private BlockJUnit4ClassRunner runner;

    public RunUntilFailure(Class<?> klass) throws InitializationError
    {
        this.runner = new BlockJUnit4ClassRunner(klass);
    }

    @Override
    public Description getDescription() {
        Description description = Description.createSuiteDescription("Run until failure");
        description.addChild(runner.getDescription());
        return description;
    }

    @Override
    public void run(RunNotifier notifier) {
        class L extends RunListener
        {
            boolean fail = false;
            public void testFailure(Failure failure) throws Exception { fail = true; }
        }
        L listener = new L();
        notifier.addListener(listener);
        while (!listener.fail) runner.run(notifier);
    }

}
