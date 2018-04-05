drop database sprinkler;
create database sprinkler;

use sprinkler;

-- Users

create table user (
	`id` int auto_increment primary key,
	`username` varchar(20) not null unique,
	`password` varchar(20) not null
);

-- Products

create table product_type (
	`id` int auto_increment primary key,
	`product_name` varchar(20) not null unique
);

create table product (
	`id` int auto_increment primary key,
	`user_id` int not null,
	`product_type_id` int not null,
	`product_key` varchar(20) not null unique,
	foreign key (`user_id`) references user(`id`),
	foreign key (`product_type_id`) references product_type(`id`)
);

create table product_heart_beat (
	`id` int auto_increment primary key,
	`product_id` int not null,
	`created` timestamp not null default current_timestamp,
		foreign key (`product_id`) references product(`id`)
);

-- Components

create table component_type (
	`id` int auto_increment primary key,
	`component_name` varchar(20) not null,
	`product_type_id` int not null,
	foreign key (`product_type_id`) references product_type(`id`),
	unique key `unique_component_product_type` (`component_name`, `product_type_id`)
);

create table component (
	`id` int auto_increment primary key,
	`component_type_id` int not null,
	`product_id` int not null,
	foreign key (`component_type_id`) references component_type(`id`),
	foreign key (`product_id`) references product(`id`),
	unique key `unique_component_type_product` (`component_type_id`, `product_id`)
);

-- Component: Valve

create table valve_state (
	`id` int auto_increment primary key,
	`component_id` int not null unique,
	`state` int not null,
	`created` timestamp not null default current_timestamp,
	foreign key (`component_id`) references component(`id`)
);

create table valve_controller_type (
	`id` int auto_increment primary key,
	`controller_name` varchar(20) not null unique
);

create table valve_timer (
	`id` int auto_increment primary key,
	`component_id` int not null unique,
	`enabled` int not null default 1,
	`start_time` time default null,
	`end_time` time default null,
	`created` timestamp not null default current_timestamp,
	foreign key (`component_id`) references component(`id`)
);

create table valve_remote_switch_job (
	`id` int auto_increment primary key,
	`component_id` int not null,
	`state` int not null,
	`completion_status` int default null,
	`created` timestamp not null default current_timestamp,
	`updated` timestamp not null default current_timestamp on update current_timestamp,
	foreign key (`component_id`) references component(`id`)
);

-- Component: Flow Sensor

create table flow_rate (
	`id` int auto_increment primary key,
	`component_id` int not null,
	`flow_volume` float not null,
	`flow_duration` float not null,
	`created` timestamp not null default current_timestamp,
	foreign key (`component_id`) references component(`id`)
);
