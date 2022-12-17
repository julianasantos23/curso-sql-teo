-- Databricks notebook source
select * from silver_olist.item_pedido


-- COMMAND ----------

with tb_vendas_vendedores as (

select 
     idVendedor,
     count(*) as qtVendas
from silver_olist.item_pedido

group by idVendedor
order by qtVendas desc

),

tb_row_number as (

select T1.*,
       T2.descUF,
       row_number() over (partition by T2.descUF order by qtVendas desc) as RN
from tb_vendas_vendedores as T1

left join silver_olist.vendedor as T2
on T1.idVendedor = T2.idVendedor

qualify RN <= 10

order by descUF, qtVendas desc
)

select * from tb_row_number


-- where filtra na fonte

-- qualify filtra window function

-- having filtra group by

-- COMMAND ----------

with tb_vendas_vendedores as (

select 
     idVendedor,
     count(*) as qtVendas
from silver_olist.item_pedido

group by idVendedor
order by qtVendas desc

),

tb_row_number as (

select T1.*,
       T2.descUF,
       row_number() over (partition by T2.descUF order by qtVendas desc) as RN
from tb_vendas_vendedores as T1

left join silver_olist.vendedor as T2
on T1.idVendedor = T2.idVendedor


order by descUF, qtVendas desc
)

select * from tb_row_number where RN <= 10
