INSERT INTO dw.dim_categoria (categoria, qtd_produtos)
SELECT categoria, count(*)
FROM raw.raw_produtos
WHERE dt_carga = %(ds)s AND categoria IS NOT NULL
GROUP BY categoria
ON CONFLICT (categoria) DO UPDATE SET
    qtd_produtos = EXCLUDED.qtd_produtos,
    atualizado_em = now();
