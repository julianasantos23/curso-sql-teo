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


