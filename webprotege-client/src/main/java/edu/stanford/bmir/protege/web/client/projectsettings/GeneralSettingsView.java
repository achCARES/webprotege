package edu.stanford.bmir.protege.web.client.projectsettings;

import com.google.gwt.user.client.ui.IsWidget;

import javax.annotation.Nonnull;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 2 Jul 2018
 */
public interface GeneralSettingsView extends IsWidget {

    void setDisplayName(@Nonnull String displayName);

    @Nonnull
    String getDisplayName();

    void setProjectEndpoint(@Nonnull String projectEndpoint);

    @Nonnull
    String getProjectEndpoint();

    void setTboxGraph(@Nonnull String tboxGraph);

    @Nonnull
    String getTboxGraph();


    void setDescription(@Nonnull String description);

    @Nonnull
    String getDescription();
}
