/*
   Defines a dataset that is observable. Meaning it a dataset that can of interest is to be watched
   and potentially sourced by downstream processes that have interest in this dataset.
*/
CREATE TABLE dataset_observer (
    /******* Immutable fields ******/
    dataset_observer_id CHAR(32) NOT NULL, -- GUID
    model_zone_tag INT NOT NULL DEFAULT 1, -- arbitrary zones that a model might move through, starting at zone 1 (optional)
    model_namespace VARCHAR(64) NOT NULL DEFAULT 'ROOT', -- optional if namespaces are desired to organize models.
    model_name VARCHAR(64) NOT NULL, -- Required logical name of the model
    -- Details about dataset and model such as partitions keys of the dataset, if any.
    model_dataset_props VARCHAR(512) NOT NULL DEFAULT 'NA',  -- **new**  can be json array of name=value and contain any properties that define the data source.
    /********************************/

    /******** Mutable fields ********/
    -- disabled: means can't not run and process/consume new sources (even if sources are ready). Sources
    -- that a ready will be returned but this observer will be marked with disabled. Will not be allowed to start.
    -- retired: like disabled plus also it can't be used to make a new source/sink dataset/observer associations.
    -- ----- Cancelled -----Note, can't make observer retired if it is being used in existing sink/source rel.
    observer_status INT NOT NULL DEFAULT 1,  -- 0 disabled, 1 enabled, 2 retired
    /* holds metadata about the dataset for use by observer process */
    observer_config VARCHAR(512),
    display_name VARCHAR(64),
    description VARCHAR(512),

    created_dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status_update_dt DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (model_zone_tag, model_namespace, model_name, model_dataset_props)
);
CREATE UNIQUE INDEX dataset_observer_indx ON dataset_observer(dataset_observer_id);

/*
   Immutable relationship between source and sink. Can only have one "active" record
   (meaning it triggers an observer to see events) per
   sink/source. Active rel is defined by the terminated_dt being NOT infinity and suspended not null.
   Rels can be terminated and new rels created over time without loss of history of rels.

   A suspended rel means it will not be "active" or visible to a subscribed observer. Events
   will be allowed to build/queue up but not made visible by subscriber/consumer/sink. Once
   unsuspended it will flood observer/subscriber/sink with new subscribed events.

   Rel must be suspended and have no unconsumed events in the queue, before it can be terminated/ended.
   Or should we allow termination if not suspended and does not have unconsumed events??
*/
CREATE TABLE dataset_source_to_sink_meta_rel (
    sink_dataset_id CHAR(32) NOT NULL,
    source_dataset_id CHAR(32) NOT NULL,
    dataset_rel_id CHAR(32) NOT NULL,  -- **new**
    suspended_dt DATETIME,  --
    terminated_dt DATETIME DEFAULT '9999-12-31 23:59:59.0', -- can never by un-terminated, can only create a new dataset_rel_id **new**
    created_dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, -- **new**

    PRIMARY KEY (dataset_rel_id)
);
CREATE UNIQUE INDEX dataset_source_to_sink_meta_rel_indx ON dataset_source_to_sink_meta_rel (sink_dataset_id, source_dataset_id, terminated_dt);
CREATE INDEX dataset_source_to_sink_meta_rel2_indx ON dataset_source_to_sink_meta_rel (source_dataset_id);

/*
   Holds records indicating a source is has events to be consumed by a sink.
*/
CREATE TABLE dataset_source_sink_event_queue (

    dataset_rel_id CHAR(32) NOT NULL, -- UUID **new**

    source_run_id CHAR(32) NOT NULL,
    sink_run_id CHAR(32) NOT NULL DEFAULT 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz',  -- this gets updated when sink observer starts running

    rerun_last_sink_run_id CHAR(32), -- UUID **new** before retry/replay make sure dataset_rel_id still exists in rel table

    rerun_status INT, -- null original not rerun, 1 replay from success, 2 retry from error, 3 retry from cleared-orphan  -- **new**

    source_ready_dt DATETIME NOT NULL, -- denormalized info
    sink_start_dt DATETIME, -- this gets updated when sink etl observer runs - denormalized info

    PRIMARY KEY (dataset_rel_id, sink_run_id, source_run_id) -- source_run_id is second intentionally?? **change**
);
CREATE INDEX dataset_source_sink_event_queue_indx ON dataset_source_sink_event_queue(source_ready_dt, dataset_rel_id); -- **new**
CREATE INDEX dataset_source_sink_event_queue2_indx ON dataset_source_sink_event_queue(sink_run_id);
CREATE INDEX dataset_source_sink_event_queue3_indx ON dataset_source_sink_event_queue(source_run_id); -- **new**

/*
   Tracks dataset observer job runs for creating/ingesting data models across and within zone models. This is
   effectively the state machine for the zone/etl orchestration. It is a wrapper around
   the external tools that can be used for the actual orchestration such as (airflow, aws step functions,
   lambda, K8, Glue....etc).

    If a dataset has downstream source observers, events are placed the queue after start and finalized once success.
   */
CREATE TABLE dataset_observer_run (
    run_id CHAR(32) NOT NULL, -- UUID
    dataset_observer_id CHAR(32) NOT NULL, -- UUID

    start_dt DATETIME NOT NULL,
    status INT NOT NULL DEFAULT 1, -- 0 error, 1 ready, 2 started, 3 success, 4 cleared-orphan (was ready/started)
    end_dt DATETIME,

    -- Persisted at start of the run automatically
    run_observer_config VARCHAR(512), -- passed config_data field through from dataset_observer at time of run.
    -- run_dataset_params NOT NULL VARCHAR(512), -- passed model_partition_keys from dataset_observer at time of run
    -- run_zone_tag INT NOT NULL, -- passed model_partition_keys from dataset_observer at time of run
    -- Customizable field at start of run that contain runtime breadcrumb trail info/metadata or metadata for retry scenarios.
    run_breadcrumb VARCHAR(512),

    -- Optional metadata persisted at the start/end of the run.
    batch_run_id VARCHAR(64), -- internal (optional) batch data run ID related to the source etl/data. This can be used by the sink dataset (if there is one) to identify records to consume.
    record_count INT, -- record count of records processed (optional)
    run_metadata VARCHAR(512), -- internal optional key and/or partition info (can/should be json) about this source etl/data that can be used by sink etl.

    -- external observer/log resource links. persisted at start or end of run
    ext_job_run_key VARCHAR(128), -- optional key to external job orchestration engine such as airflow
    ext_job_run_output_log_link VARCHAR(256), -- e.g. cloudwatch log link
    ext_etl_proc_key VARCHAR(128), -- optional key to external etl engine (lambda, k8, glue..)
    ext_etl_proc_output_log_link VARCHAR(256), -- e.g. if using lambda, cloudwatch log

    PRIMARY KEY (run_id) -- must be unique
);
CREATE INDEX dataset_observer_run_indx ON dataset_observer_run(dataset_observer_id, start_dt); -- for joins
CREATE INDEX dataset_observer_run2_indx ON dataset_observer_run(start_dt, dataset_observer_id); -- for joins
CREATE INDEX dataset_observer_run3_indx ON dataset_observer_run(start_dt, status); -- for joins