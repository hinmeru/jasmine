# Join

## Built-in functions

### aj

```
aj(on, df1, df2)
```

- on: cats
- df1: dataframe
- df2: dataframe

```
gdp0 = df[
  country=`Germany`Germany`Germany`Germany`Germany`Netherlands`Netherlands`Netherlands`Netherlands`Netherlands,
  date=2016-01-01 2017-01-01 2018-01-01 2019-01-01 2020-01-01 2016-01-01 2017-01-01 2018-01-01 2019-01-01 2020-01-01,
  gdp=4164 4411 4566 4696 4827 784 833 914 910 909,
];

pop0 = df[
  country=`Germany`Germany`Germany`Netherlands`Netherlands`Netherlands,
  date=2016-03-01 2018-08-01 2019-01-01 2016-03-01 2018-08-01 2019-01-01,
  population=82.19 82.66 83.12 17.11 17.32 17.40,
];

aj(`country`date, pop0, gdp0)
```
