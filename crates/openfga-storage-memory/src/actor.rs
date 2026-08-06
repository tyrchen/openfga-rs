//! Bounded actor command protocol and ownership loop.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use openfga_domain::{AuthorizationModelId, RelationshipTuple, StoreId, TupleKey};
use openfga_storage::{
    Assertion, ChangeFilter, HealthStatus, MutationOutcome, ObjectRelationFilter, OperationContext,
    Page, PageOptions, ReadOptions, ReverseTupleFilter, StorageError, StoreFilter, StoreName,
    StoreRecord, StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter,
    TupleWriteOptions, UsersetTupleFilter,
};
use tokio::sync::{mpsc, oneshot};

use crate::state::MemoryState;

pub(crate) type Reply<T> = oneshot::Sender<Result<T, StorageError>>;

#[derive(Debug)]
pub(crate) enum ActorMessage {
    Operation(Box<Envelope>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub(crate) struct Envelope {
    pub(crate) context: OperationContext,
    pub(crate) command: Command,
}

#[derive(Debug)]
pub(crate) enum Command {
    ReadTuples {
        store_id: StoreId,
        filter: TupleReadFilter,
        options: PageOptions,
        reply: Reply<Page<StoredTuple>>,
    },
    ReadExact {
        store_id: StoreId,
        key: TupleKey,
        reply: Reply<StoredTuple>,
    },
    ReadObjectRelation {
        store_id: StoreId,
        filter: ObjectRelationFilter,
        options: ReadOptions,
        reply: Reply<Vec<RelationshipTuple>>,
    },
    ReadUserset {
        store_id: StoreId,
        filter: UsersetTupleFilter,
        options: ReadOptions,
        reply: Reply<Vec<RelationshipTuple>>,
    },
    ReadReverse {
        store_id: StoreId,
        filter: ReverseTupleFilter,
        options: ReadOptions,
        reply: Reply<Vec<RelationshipTuple>>,
    },
    TupleExists {
        store_id: StoreId,
        key: TupleKey,
        reply: Reply<bool>,
    },
    CountObjectRelation {
        store_id: StoreId,
        filter: ObjectRelationFilter,
        reply: Reply<u64>,
    },
    WriteTuples {
        store_id: StoreId,
        deletes: Vec<TupleKey>,
        writes: Vec<RelationshipTuple>,
        options: TupleWriteOptions,
        reply: Reply<MutationOutcome>,
    },
    ReadModel {
        store_id: StoreId,
        model_id: AuthorizationModelId,
        reply: Reply<Arc<StoredAuthorizationModel>>,
    },
    ReadLatestModel {
        store_id: StoreId,
        reply: Reply<Arc<StoredAuthorizationModel>>,
    },
    ListModels {
        store_id: StoreId,
        options: PageOptions,
        reply: Reply<Page<Arc<StoredAuthorizationModel>>>,
    },
    WriteModel {
        model: Arc<StoredAuthorizationModel>,
        reply: Reply<()>,
    },
    ReadStore {
        store_id: StoreId,
        reply: Reply<StoreRecord>,
    },
    ListStores {
        filter: StoreFilter,
        options: PageOptions,
        reply: Reply<Page<StoreRecord>>,
    },
    CreateStore {
        store_id: StoreId,
        name: StoreName,
        reply: Reply<StoreRecord>,
    },
    RenameStore {
        store_id: StoreId,
        name: StoreName,
        reply: Reply<StoreRecord>,
    },
    DeleteStore {
        store_id: StoreId,
        reply: Reply<()>,
    },
    ReadAssertions {
        store_id: StoreId,
        model_id: AuthorizationModelId,
        reply: Reply<Arc<[Assertion]>>,
    },
    WriteAssertions {
        store_id: StoreId,
        model_id: AuthorizationModelId,
        assertions: Vec<Assertion>,
        reply: Reply<()>,
    },
    ReadChanges {
        store_id: StoreId,
        filter: ChangeFilter,
        options: PageOptions,
        reply: Reply<Page<TupleChange>>,
    },
    Health {
        reply: Reply<HealthStatus>,
    },
}

impl Command {
    fn fail(self, error: StorageError) {
        match self {
            Self::ReadTuples { reply, .. } => send(reply, Err(error)),
            Self::ReadExact { reply, .. } => send(reply, Err(error)),
            Self::ReadObjectRelation { reply, .. }
            | Self::ReadUserset { reply, .. }
            | Self::ReadReverse { reply, .. } => send(reply, Err(error)),
            Self::TupleExists { reply, .. } => send(reply, Err(error)),
            Self::CountObjectRelation { reply, .. } => send(reply, Err(error)),
            Self::WriteTuples { reply, .. } => send(reply, Err(error)),
            Self::ReadModel { reply, .. } | Self::ReadLatestModel { reply, .. } => {
                send(reply, Err(error));
            }
            Self::ListModels { reply, .. } => send(reply, Err(error)),
            Self::WriteModel { reply, .. }
            | Self::DeleteStore { reply, .. }
            | Self::WriteAssertions { reply, .. } => send(reply, Err(error)),
            Self::ReadStore { reply, .. } => send(reply, Err(error)),
            Self::ListStores { reply, .. } => send(reply, Err(error)),
            Self::CreateStore { reply, .. } | Self::RenameStore { reply, .. } => {
                send(reply, Err(error));
            }
            Self::ReadAssertions { reply, .. } => send(reply, Err(error)),
            Self::ReadChanges { reply, .. } => send(reply, Err(error)),
            Self::Health { reply } => send(reply, Err(error)),
        }
    }
}

pub(crate) async fn run_actor(
    mut receiver: mpsc::Receiver<ActorMessage>,
    mut state: MemoryState,
    running: Arc<AtomicBool>,
) {
    running.store(true, Ordering::Release);
    while let Some(message) = receiver.recv().await {
        let envelope = match message {
            ActorMessage::Operation(envelope) => *envelope,
            ActorMessage::Shutdown(reply) => {
                let _ = reply.send(());
                break;
            }
        };
        if let Err(error) = envelope.context.check() {
            envelope.command.fail(error);
            continue;
        }
        handle_command(&mut state, envelope.command, &envelope.context);
    }
    running.store(false, Ordering::Release);
}

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive actor protocol keeps every state transition in one owner turn"
)]
fn handle_command(state: &mut MemoryState, command: Command, context: &OperationContext) {
    match command {
        Command::ReadTuples {
            store_id,
            filter,
            options,
            reply,
        } => send(reply, state.read_tuples(store_id, &filter, &options)),
        Command::ReadExact {
            store_id,
            key,
            reply,
        } => send(reply, state.read_exact(store_id, &key)),
        Command::ReadObjectRelation {
            store_id,
            filter,
            options,
            reply,
        } => send(
            reply,
            state.read_object_relation(store_id, &filter, options),
        ),
        Command::ReadUserset {
            store_id,
            filter,
            options,
            reply,
        } => send(reply, state.read_userset(store_id, &filter, options)),
        Command::ReadReverse {
            store_id,
            filter,
            options,
            reply,
        } => send(reply, state.read_reverse(store_id, &filter, options)),
        Command::TupleExists {
            store_id,
            key,
            reply,
        } => send(reply, Ok(state.tuple_exists(store_id, &key))),
        Command::CountObjectRelation {
            store_id,
            filter,
            reply,
        } => send(reply, state.count_object_relation(store_id, &filter)),
        Command::WriteTuples {
            store_id,
            deletes,
            writes,
            options,
            reply,
        } => send(
            reply,
            state.write_tuples(context, store_id, deletes, writes, options),
        ),
        Command::ReadModel {
            store_id,
            model_id,
            reply,
        } => send(reply, state.read_model(store_id, model_id)),
        Command::ReadLatestModel { store_id, reply } => {
            send(reply, state.read_latest_model(store_id));
        }
        Command::ListModels {
            store_id,
            options,
            reply,
        } => send(reply, state.list_models(store_id, &options)),
        Command::WriteModel { model, reply } => send(reply, state.write_model(model)),
        Command::ReadStore { store_id, reply } => send(reply, state.read_store(store_id)),
        Command::ListStores {
            filter,
            options,
            reply,
        } => send(reply, state.list_stores(&filter, &options)),
        Command::CreateStore {
            store_id,
            name,
            reply,
        } => send(reply, state.create_store(store_id, name)),
        Command::RenameStore {
            store_id,
            name,
            reply,
        } => send(reply, state.rename_store(store_id, name)),
        Command::DeleteStore { store_id, reply } => {
            state.delete_store(store_id);
            send(reply, Ok(()));
        }
        Command::ReadAssertions {
            store_id,
            model_id,
            reply,
        } => send(reply, state.read_assertions(store_id, model_id)),
        Command::WriteAssertions {
            store_id,
            model_id,
            assertions,
            reply,
        } => send(
            reply,
            state.write_assertions(store_id, model_id, assertions),
        ),
        Command::ReadChanges {
            store_id,
            filter,
            options,
            reply,
        } => send(reply, state.read_changes(store_id, &filter, &options)),
        Command::Health { reply } => send(reply, Ok(HealthStatus::new(true, "ready"))),
    }
}

fn send<T>(reply: Reply<T>, result: Result<T, StorageError>) {
    let _ = reply.send(result);
}
