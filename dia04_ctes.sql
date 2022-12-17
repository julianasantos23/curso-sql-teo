-- Databricks notebook source
-- Lista de vendedores que estão no estado com menos clientes

select descUF,
       count(distinct idClienteUnico)

from silver_olist.cliente

group by descUF
order by count(distinct idClienteUnico) desc

-- COMMAND ----------

select idVendedor, descUF
from silver_olist.vendedor
where descUF = (
       select descUF
       from silver_olist.cliente
       group by descUF
       order by count(distinct idClienteUnico) desc
       limit 1
                )

-- COMMAND ----------

select idVendedor, descUF
from silver_olist.vendedor
where descUF in(

       select descUF
       from silver_olist.cliente
       group by descUF
       order by count(distinct idClienteUnico) desc
       limit 2
                )

-- COMMAND ----------

with tb_estados as (

       select descUF
       from silver_olist.cliente
       group by descUF
       order by count(distinct idClienteUnico) desc
       limit 2
       
)

select idVendedor,
       descUF
       
from silver_olist.vendedor
where descUF in (select * from tb_estados)

-- COMMAND ----------

with tb_estados as (

       select descUF
       from silver_olist.cliente
       group by descUF
       order by count(distinct idClienteUnico) desc
       limit 2
       
),

tb_vendedores as (
  select idVendedor,descUF
  from silver_olist.vendedor
  where descUF in (select * from tb_estados)
)

select *
from tb_vendedores

-- COMMAND ----------

with tb_pedido_nota as (

select T1.idVendedor,T2.vlNota
from silver_olist.item_pedido as T1

left join silver_olist.avaliacao_pedido as T2
on T1.idPedido = T2.idPedido
),

tb_avg_vendedor as (

select idVendedor,
       AVG(vlNota) as avgNotaVendedor
from tb_pedido_nota
group by idVendedor
),

tb_vendedor_estado as(
 select T1.*,
        T2.descUF
 from tb_avg_vendedor as T1
 left join silver_olist.vendedor as T2
 on T1.idVendedor = T2.idVendedor

)

select descUF,
       AVG(avgNotaVendedor) as avgNotaEstado
       
from tb_vendedor_estado

group by descUF
order by avgNotaEstado desc
      

-- COMMAND ----------


