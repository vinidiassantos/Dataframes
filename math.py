# Criando um novo DataFrame com os dados que serão inseridos ou atualizados
novo_despachante = spark.createDataFrame([(1, "João", "Ativo", "São Paulo", 10000, "2023-07-05"),
                                          (11, "Maria", "Inativo", "Rio de Janeiro", 5000, "2023-07-05")],
                                         ["id", "nome", "status", "cidade", "vendas", "data"])

# Especificando o caminho para o diretório Delta Lake onde os dados serão armazenados
delta_path = "/path/despachantes"

# Salvando o DataFrame original no formato Delta Lake
despachantes_df.write.format("delta").mode("overwrite").save(delta_path)

# Carregando o DeltaTable a partir do caminho especificado
delta_table = DeltaTable.forPath(spark, delta_path)

# Definindo a condição de merge (como exemplo, vamos usar a coluna "id")
condition = "target.id = source.id"

# Executando o merge/upsert
delta_table.alias("target") \
    .merge(novo_despachante.alias("source"), condition) \
    .whenMatchedUpdate(set={"nome": "source.nome", "status": "source.status", "cidade": "source.cidade",
                            "vendas": "source.vendas", "data": "source.data"}) \
    .whenNotMatchedInsert(values={"id": "source.id", "nome": "source.nome", "status": "source.status",
                                  "cidade": "source.cidade", "vendas": "source.vendas", "data": "source.data"}) \
    .execute()

# Lendo o DataFrame resultante após o merge/upsert
despachantes_atualizados_df = spark.read.format("delta").load(delta_path)

# Exibindo o DataFrame resultante
despachantes_atualizados_df.orderBy("id").show()
