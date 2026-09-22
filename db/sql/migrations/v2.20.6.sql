create table `uniflow_ee_registry` (
  `id` integer primary key autoincrement,
  `project_id` int not null,
  `name` varchar(255) not null,
  `url` varchar(512) not null,
  `username` varchar(255),
  `password` varchar(512),
  `created` datetime not null,
  foreign key (`project_id`) references `project`(`id`) on delete cascade
);

create table `uniflow_execution_environment` (
  `id` integer primary key autoincrement,
  `project_id` int not null,
  `name` varchar(255) not null,
  `description` text,
  `image` varchar(512) not null,
  `pull_policy` varchar(32) not null default 'IfNotPresent',
  `registry_id` int,
  `created` datetime not null,
  `updated` datetime not null,
  foreign key (`project_id`) references `project`(`id`) on delete cascade,
  foreign key (`registry_id`) references `uniflow_ee_registry`(`id`) on delete set null
);

alter table `project__template` add column `ee_id` int null references `uniflow_execution_environment`(`id`) on delete set null;
