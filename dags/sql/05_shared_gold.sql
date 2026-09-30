-- 05 Shared/Gold: venda por item + atributos do produto e do cliente da mesma carga.
-- Idempotente: upsert por (id_carrinho, id_produto).
INSERT INTO shared.vendas_consolidadas AS s (
    id_carrinho, id_produto, data_venda, nome_produto, categoria, marca,
    id_cliente, nome_cliente, cidade, uf, faixa_etaria,
    quantidade, preco, valor_bruto, valor_desconto, valor_total, dt_carga
)
SELECT
    v.id_carrinho, v.id_produto, v.data_venda,
    COALESCE(p.nome_produto, v.nome_produto), p.categoria, p.marca,
    v.id_cliente, c.nome_completo, c.cidade, c.uf, c.faixa_etaria,
    v.quantidade, v.preco, v.valor_bruto, v.valor_desconto, v.valor_total, v.dt_carga
FROM raw.raw_vendas v
LEFT JOIN raw.raw_produtos p ON p.id_produto = v.id_produto AND p.dt_carga = v.dt_carga
LEFT JOIN raw.raw_clientes c ON c.id_cliente = v.id_cliente AND c.dt_carga = v.dt_carga
WHERE v.dt_carga = %(ds)s
ON CONFLICT (id_carrinho, id_produto) DO UPDATE SET
    data_venda = EXCLUDED.data_venda,       nome_produto = EXCLUDED.nome_produto,
    categoria = EXCLUDED.categoria,         marca = EXCLUDED.marca,
    id_cliente = EXCLUDED.id_cliente,       nome_cliente = EXCLUDED.nome_cliente,
    cidade = EXCLUDED.cidade,               uf = EXCLUDED.uf,
    faixa_etaria = EXCLUDED.faixa_etaria,   quantidade = EXCLUDED.quantidade,
    preco = EXCLUDED.preco,                 valor_bruto = EXCLUDED.valor_bruto,
    valor_desconto = EXCLUDED.valor_desconto, valor_total = EXCLUDED.valor_total,
    dt_carga = EXCLUDED.dt_carga;
