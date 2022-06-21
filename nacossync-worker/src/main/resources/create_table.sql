create table cluster
(
    id               integer generated by default as identity default on null not null,
    cluster_id       varchar(255),
    cluster_name     varchar(255),
    cluster_type     varchar(255),
    connect_key_list varchar(255),
    namespace        varchar(255),
    password         varchar(255),
    user_name        varchar(255),
    primary key (id)
);

create table system_config
(
    id           bigint generated by default as identity default on null not null,
    config_desc  varchar(255),
    config_key   varchar(255),
    config_value varchar(255),
    primary key (id)
);

create table task
(
    id                bigint generated by default as identity default on null not null,
    dest_cluster_id   varchar(255),
    group_name        varchar(255),
    name_space        varchar(255),
    operation_id      varchar(255),
    service_name      varchar(255),
    source_cluster_id varchar(255),
    task_id           varchar(255),
    task_status       varchar(255),
    version           varchar(255),
    worker_ip         varchar(255),
    primary key (id)
);