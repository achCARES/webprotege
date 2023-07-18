package edu.stanford.bmir.protege.web.server.change;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.factory.AutoFactory;
import com.google.auto.factory.Provided;
import edu.stanford.bmir.protege.web.server.owlapi.RenameMap;
import edu.stanford.bmir.protege.web.server.revision.Revision;
import edu.stanford.bmir.protege.web.server.revision.RevisionManager;
import edu.stanford.bmir.protege.web.shared.revision.RevisionNumber;
import java.util.ArrayList;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.inject.Inject;

/**
 * Matthew Horridge
 * Stanford Center for Biomedical Informatics Research
 * 19/03/15
 */
public class RevisionAddedChangeListGenerator implements ChangeListGenerator<Boolean> {

    @Nonnull
    private final RevisionNumber revisionNumber;

    @Nonnull
    private final RevisionManager revisionManager;

    @AutoFactory
    @Inject
    public RevisionAddedChangeListGenerator(@Nonnull RevisionNumber revisionNumber,
                                               @Provided @Nonnull RevisionManager revisionManager) {
        this.revisionNumber = checkNotNull(revisionNumber);
        this.revisionManager = checkNotNull(revisionManager);
    }

    @Override
    public Boolean getRenamedResult(Boolean result, RenameMap renameMap) {
        return result;
    }

    @Override
    public OntologyChangeList<Boolean> generateChanges(ChangeGenerationContext context) {
        Optional<Revision> revision = revisionManager.getRevision(revisionNumber);
        if(revision.isEmpty()) {
            return OntologyChangeList.<Boolean>builder().build(false);
        }
        ArrayList<OntologyChange> changes = new ArrayList<>();
        Revision theRevision = revision.get();
        for(OntologyChange change : theRevision.getChanges()) {
            changes.add(change);
        }
        return OntologyChangeList.<Boolean>builder().addAll(changes).build(true);
    }

    @Nonnull
    @Override
    public String getMessage(ChangeApplicationResult<Boolean> result) {
        return "Added revision " + revisionNumber.getValue();
    }
}
