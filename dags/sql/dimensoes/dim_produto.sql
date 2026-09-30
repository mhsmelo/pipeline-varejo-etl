-- Todos os produtos da carga (não só os vendidos), para o catálogo ficar completo.
INSERT INTO dw.dim_produto (id_produto, nome_produto, categoria, marca, sku, preco_atual)
SELECT id_produto, nome_produto, categoria, marca, sku, preco
FROM raw.raw_produtos
WHERE dt_carga = %(ds)s
ON CONFLICT (id_produto) DO UPDATE SET
    nome_produto = EXCLUDED.nome_produto, categoria = EXCLUDED.categoria,
    marca = EXCLUDED.marca, sku = EXCLUDED.sku, preco_atual = EXCLUDED.preco_atual,
    atualizado_em = now();
