-- Databricks notebook source
select * 

from silver_olist.pedido

where descSituacao = 'shipped' 
and year(dtPedido) = '2018'

-- COMMAND ----------

select * 

from silver_olist.pedido

where (descSituacao = 'shipped' or descSituacao = 'canceled') 
and year(dtPedido) = '2018'

--leia-se : Selecione todas as colunas da tabela silver_olist.pedidos filtrando pedidos (enviados ou cancelados)de 2018 Utilizando OR

-- COMMAND ----------

select * 

from silver_olist.pedido

where descSituacao in ('shipped','canceled')
and year(dtPedido) = '2018'

--leia-se : Selecione todas as colunas da tabela silver_olist.pedidos filtrando pedidos (enviados ou cancelados)de 2018 Utilizando o IN

-- COMMAND ----------

select * ,
  datediff(dtEstimativaEntrega, dtAprovado) as diffDatasAprovadasEntregues

from silver_olist.pedido

where descSituacao in ('shipped','canceled')
and year(dtPedido) = '2018'
and datediff(dtEstimativaEntrega, dtAprovado) > 30

-- COMMAND ----------

select 
        T3.descUF,
        AVG(T1.vlFrete) as avgFrete

from silver_olist.item_pedido as T1

left join silver_olist.pedido as T2
on T1.idPedido = T2.idPedido

left join silver_olist.cliente as T3
on T2.idCliente = T3.idCliente

group by T3.descUF

having AVG(T1.vlFrete) > 40

order by avgFrete


-- COMMAND ----------


