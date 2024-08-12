
 CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NULL
);
SELECT create_hypertable('stocks', by_range('time'));

CREATE TABLE company (                                                      
  symbol TEXT NOT NULL,
  name TEXT NOT NULL
);

-- para cada key (symbol nesse caso), criar uma tabela especifica (como acima ???)


 CREATE TABLE ticond (
  time TIMESTAMP NOT NULL,
  acid DOUBLE PRECISION NOT NULL,
  brigt DOUBLE PRECISION NOT NULL,
  chem DOUBLE PRECISION NOT NULL
);