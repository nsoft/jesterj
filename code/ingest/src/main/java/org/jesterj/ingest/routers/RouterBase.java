package org.jesterj.ingest.routers;

import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.NamedBuilder;

public abstract class RouterBase implements Router {
    Step step;
    String name;

    @Override
    public Step getStep() {
      return step;
    }

    public abstract static class Builder<T extends RouterBase> extends NamedBuilder<RouterBase> {
        private T obj;

        public RouterBase.Builder<T> named(String name) {
            getObj().name = name;
            return this;
        }

        public RouterBase.Builder<T> forStep(Step step) {
            getObj().step = step;
            return this;
        }

        protected T getObj() {
            return obj;
        }

        public T build() {
            obj = null;
            return null; // abstract class
        }
    }

}
