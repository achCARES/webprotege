package edu.stanford.bmir.protege.web.server.owlapi;

import javax.annotation.Nonnull;

public interface OWLLoaderFactory {


    public OWLLoader createLoader();

    public OWLLoader get();

}
