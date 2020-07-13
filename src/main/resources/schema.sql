create table eventMessages
(
   id integer auto_increment not null,
   description varchar(255) not null,
   severity varchar(6) not null,
   primary key(id)
);
