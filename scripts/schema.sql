use pricing;
drop table if exists groceries;
create table if not exists groceries (
	item varchar(255),
    price decimal (6,2),
    unit varchar(255),
    store varchar(255),
    zipcode varchar(10),
    price_date varchar(255)
);
