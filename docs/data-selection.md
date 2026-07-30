```
# Selección de columnas para el Bronze Layer

## Contexto

El dataset de Lending Club (Accepted Loans, 2007–2018 Q4) contiene 2.260.701 préstamos y 151 columnas. Antes de empezar a construir el pipeline, quise entender qué información realmente necesitaba y qué podía dejar afuera.

La idea no era cargar todas las columnas "por las dudas", sino quedarme con las que aportan valor para el objetivo del proyecto. Además de simplificar el modelo de datos, esto hace que el pipeline sea más liviano y fácil de mantener.

## Cómo decidí qué columnas conservar

Para hacer la selección seguí tres criterios bastante simples.

1. Que aporten al análisis que quiero construir.

El proyecto está orientado a analizar el ciclo de vida de un préstamo. Por eso prioricé las columnas necesarias para calcular métricas como:

- Originación de préstamos
- Vintage curves
- Roll rates
- Recovery rate
- Portfolio at Risk (PAR)

Si una columna no ayudaba a construir alguna de estas métricas, probablemente no tenía sentido incluirla.

2. La cantidad de valores faltantes.

También revisé el porcentaje de valores nulos. Cuando una columna tenía más del 90% de nulls, normalmente la descartaba, salvo que fuera realmente importante para el análisis.

3. El alcance del proyecto.

Este proyecto busca mostrar conceptos de Data Engineering, no desarrollar un modelo de credit scoring.

Por esa razón dejé afuera gran parte de las variables provenientes del bureau de crédito. Son datos muy útiles para modelos predictivos, pero agregaban mucha complejidad sin aportar demasiado al objetivo principal.

---

## Columnas descartadas

### Información de hardship

La familia de columnas hardship_* tiene aproximadamente un 99.5% de valores nulos.

Estos campos registran los planes especiales de pago que se ofrecen cuando un cliente atraviesa dificultades financieras. Como apenas alrededor del 0.5% de los préstamos pasó por esa situación, no tiene sentido mantener todas esas columnas en la tabla principal.

Si en algún momento quisiera analizar esos casos, prefiero modelarlos como una tabla independiente, por ejemplo fact_hardship_events.

### Información de settlement

Algo parecido ocurre con las columnas settlement_*.

Representan acuerdos de pago posteriores a un charge-off y solamente aparecen en aproximadamente el 1.5% de los préstamos.

También decidí dejarlas fuera del modelo principal y tratarlas como un dominio separado si alguna vez hiciera un análisis específico de recuperaciones.

### Información de aplicaciones conjuntas

Las columnas sec_app_* y los campos terminados en *_joint contienen información del co-solicitante cuando el préstamo fue solicitado por dos personas.

Este tipo de préstamos representa apenas cerca del 5% del total, así que para este proyecto no justifican la complejidad adicional.

### Campos obsoletos o poco útiles

También descarté algunos campos que hoy prácticamente no aportan información.

- member_id está completamente vacío porque Lending Club dejó de publicarlo por cuestiones de privacidad.
- desc contiene una descripción escrita por el solicitante, pero además de tener un porcentaje muy alto de valores nulos, fue eliminado en versiones posteriores del dataset.
- url corresponde a un formato antiguo y ya no tiene utilidad para el análisis.

### Variables del bureau de crédito

El dataset incluye más de cincuenta columnas relacionadas con el historial crediticio del cliente (num_tl_*, bc_*, mo_sin_*, open_acc_*, il_util, entre otras).

Son variables muy valiosas para construir modelos de riesgo o credit scoring, pero ese no es el foco de este proyecto.

Como el objetivo es analizar el comportamiento de los préstamos y demostrar buenas prácticas de ingeniería de datos, preferí mantener un modelo más simple y fácil de entender.

---

## Columnas seleccionadas

Después de este proceso terminé conservando 33 columnas, agrupadas según el tipo de información que representan.

### Identificadores

- id

### Información del préstamo

- loan_amnt
- funded_amnt
- term
- int_rate
- installment
- grade
- sub_grade

### Información del solicitante

- emp_title
- emp_length
- home_ownership
- annual_inc
- verification_status
- dti
- application_type

### Origen del préstamo

- purpose
- title
- zip_code
- addr_state

### Fechas importantes

- issue_d
- last_pymnt_d
- earliest_cr_line

### Estado y comportamiento del préstamo

- loan_status
- pymnt_plan
- out_prncp
- total_pymnt
- total_rec_prncp
- total_rec_int
- total_rec_late_fee
- recoveries

### Indicadores crediticios

- fico_range_low
- fico_range_high
- revol_util

---

## Qué permite hacer esta selección

| Métrica | Columnas utilizadas |
|----------|---------------------|
| Volumen y mix de originación | issue_d, loan_amnt, grade, purpose, addr_state |
| Vintage curves | issue_d, loan_status, last_pymnt_d |
| Roll rates | loan_status, last_pymnt_d, out_prncp |
| Recovery rate | recoveries, total_rec_prncp, loan_status |
| Portfolio at Risk | out_prncp, loan_status |
| Segmentación | grade, sub_grade, fico_range_low, dti, addr_state |

---

## Mirando un poco más adelante

La selección de columnas responde al alcance actual del proyecto y no significa que el resto de la información no sea útil.

Si más adelante quisiera construir un modelo de riesgo crediticio, seguramente volvería a incorporar buena parte de las variables del bureau de crédito.

De la misma manera, si apareciera la necesidad de analizar procesos de hardship o settlement, esos datos podrían integrarse como tablas específicas del dominio, manteniendo la tabla principal enfocada únicamente en la información esencial del préstamo.
```
