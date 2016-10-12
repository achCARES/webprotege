package edu.stanford.bmir.protege.web.shared.issues;

import com.google.gwt.event.shared.EventHandler;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 11 Oct 2016
 */
public interface CommentPostedHandler extends EventHandler {

    void handleCommentPosted(@Nonnull CommentPostedEvent commentPostedEvent);
}
