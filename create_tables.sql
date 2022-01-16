create table location_mapping
(
    DeviceID     int not null
        constraint location_mapping_pk
            primary key,
    LocationName varchar(255)
)
go

create table signal_mapping
(
    SignalID int not null
        constraint signal_mapping_pk
            primary key,
    Signal   varchar(255)
)
go

create table weather_data
(
    ts       varchar(255) not null,
    value    float,
    SignalID int          not null
        constraint FK_signal_mapping
            references signal_mapping,
    DeviceID int          not null
        constraint FK_location_mapping
            references location_mapping,
    constraint PK_weather_data
        primary key (SignalID, DeviceID, ts)
)
go

