-- Initial table creation
create table peers (
    id serial not null,
    name varchar,
    constraint peers_pkey primary key (id)
);