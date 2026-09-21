# Fonctions de fenêtrage (Window functions)

## 1. Syntaxe de base

```sql
SELECT
  val,
  ROW_NUMBER() OVER (ORDER BY val) AS 'row_number',
  RANK()       OVER (ORDER BY val) AS 'rank',
  DENSE_RANK() OVER (ORDER BY val) AS 'dense_rank'
FROM numbers;
```

```sql
SELECT
         val,
         ROW_NUMBER()   OVER w AS 'row_number',
         CUME_DIST()    OVER w AS 'cume_dist',
         PERCENT_RANK() OVER w AS 'percent_rank'
       FROM numbers
       WINDOW w AS (ORDER BY val);
```

## 2. Liste des fonctions


| Name | Description |
|---|---|
| CUME_DIST() | Valeur cumulée de la distribution des données |
| DENSE_RANK() | Rang de la ligne dans sa propre partition, sans intervalle |
| FIRST_VALUE() | Valeur de la première ligne de la fenêtre |
| LAG() | Valeur de la conne selon d'une ligne distante d'un intervalle donnée de la ligne courante, à l'intérieur d'une partition |
| LAST_VALUE() | Valeur de la dernière ligne de la fenêtre |
| LEAD() | Valaur de la colonne pour la ligne précédeant la ligne courante |
| NTH_VALUE() | Valeur de la colonne pour la n-ième ligne de la partition |
| NTILE() | Numéro du groupe de la ligne à l'interieur d'une partition |
| PERCENT_RANK() | Valeur du rang en pourcentage |
| RANK() | Rang de la ligne dans sa propre partition, avec intervalle |
| ROW_NUMBER() | Numéro de la ligne à l'intérieur de sa propre partition  |

## 3. Partitions

#### Exemple 1
```sql
SELECT
         t, val,
         LAG(val)        OVER w AS 'lag',
         LEAD(val)       OVER w AS 'lead',
         val - LAG(val)  OVER w AS 'lag diff',
         val - LEAD(val) OVER w AS 'lead diff'
       FROM series
       WINDOW w AS (ORDER BY t);
```
#### Résultat
```
+----------+------+------+------+----------+-----------+
| t        | val  | lag  | lead | lag diff | lead diff |
+----------+------+------+------+----------+-----------+
| 12:00:00 |  100 | NULL |  125 |     NULL |       -25 |
| 13:00:00 |  125 |  100 |  132 |       25 |        -7 |
| 14:00:00 |  132 |  125 |  145 |        7 |       -13 |
| 15:00:00 |  145 |  132 |  140 |       13 |         5 |
| 16:00:00 |  140 |  145 |  150 |       -5 |       -10 |
| 17:00:00 |  150 |  140 |  200 |       10 |       -50 |
| 18:00:00 |  200 |  150 | NULL |       50 |      NULL |
+----------+------+------+------+----------+-----------+
```
### Exemple 2
```sql
SELECT
         time, subject, val,
         FIRST_VALUE(val)  OVER w AS 'first',
         LAST_VALUE(val)   OVER w AS 'last',
         NTH_VALUE(val, 2) OVER w AS 'second',
         NTH_VALUE(val, 4) OVER w AS 'fourth'
       FROM observations
       WINDOW w AS (PARTITION BY subject ORDER BY time ROWS UNBOUNDED PRECEDING);
```
#### Résultat
```
+----------+---------+------+-------+------+--------+--------+
| time     | subject | val  | first | last | second | fourth |
+----------+---------+------+-------+------+--------+--------+
| 07:00:00 | st113   |   10 |    10 |   10 |   NULL |   NULL |
| 07:15:00 | st113   |    9 |    10 |    9 |      9 |   NULL |
| 07:30:00 | st113   |   25 |    10 |   25 |      9 |   NULL |
| 07:45:00 | st113   |   20 |    10 |   20 |      9 |     20 |
| 07:00:00 | xh458   |    0 |     0 |    0 |   NULL |   NULL |
| 07:15:00 | xh458   |   10 |     0 |   10 |     10 |   NULL |
| 07:30:00 | xh458   |    5 |     0 |    5 |     10 |   NULL |
| 07:45:00 | xh458   |   30 |     0 |   30 |     10 |     30 |
| 08:00:00 | xh458   |   25 |     0 |   25 |     10 |     30 |
+----------+---------+------+-------+------+--------+--------+
```

## 4. Partitions internes
```sql
SELECT
         year, country, product, profit,
         SUM(profit) OVER() AS total_profit,
         SUM(profit) OVER(PARTITION BY country) AS country_profit
       FROM sales
       ORDER BY country, year, product, profit;
```
### Fonctions d'agrégation

- AVG()
- BIT_AND()
- BIT_OR()
- BIT_XOR()
- COUNT()
- JSON_ARRAYAGG()
- JSON_OBJECTAGG()
- MAX()
- MIN()
- STDDEV_POP(), STDDEV(), STD()
- STDDEV_SAMP()
- SUM()
- VAR_POP(), VARIANCE()
- - $AR_SAMP()

## 5. Syntaxe avancée

```sql
SELECT
         time, subject, val,
         SUM(val) OVER (PARTITION BY subject ORDER BY time
                        ROWS UNBOUNDED PRECEDING)
           AS running_total,
         AVG(val) OVER (PARTITION BY subject ORDER BY time
                        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
           AS running_average
       FROM observations;
```
