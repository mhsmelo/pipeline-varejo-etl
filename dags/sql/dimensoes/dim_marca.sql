-- Parte dos produtos da DummyJSON não tem marca: eles entram como 'Sem marca'.
INSERT INTO dw.dim_marca (marca, qtd_produtos)
SELECT COALESCE(marca, 'Sem marca'), count(*)
FROM raw.raw_produtos
WHERE dt_carga = %(ds)s
GROUP BY COALESCE(marca, 'Sem marca')
ON CONFLICT (marca) DO UPDATE SET
    qtd_produtos = EXCLUDED.qtd_produtos,
    atualizado_em = now();
