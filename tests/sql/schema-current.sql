CREATE TABLE table_one (
    id INT NOT NULL PRIMARY KEY,
    value varchar(64) NOT NULL
);
INSERT INTO table_one (id, value) VALUES (1, 'foo');
INSERT INTO table_one (id, value) VALUES (2, 'bar');
INSERT INTO table_one (id, value) VALUES (3, 'baz');

CREATE TABLE table_two (
    t_id INT NOT NULL PRIMARY KEY,
    value varchar(64) NOT NULL
);
INSERT INTO table_two (t_id, value) VALUES (4, 'foofoo');
INSERT INTO table_two (t_id, value) VALUES (5, 'foobar');
