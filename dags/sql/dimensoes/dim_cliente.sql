INSERT INTO dw.dim_cliente (id_cliente, nome_completo, email, genero, faixa_etaria,
                            cidade, estado, uf, pais)
SELECT id_cliente, nome_completo, email, genero, faixa_etaria, cidade, estado, uf, pais
FROM raw.raw_clientes
WHERE dt_carga = %(ds)s
ON CONFLICT (id_cliente) DO UPDATE SET
    nome_completo = EXCLUDED.nome_completo, email = EXCLUDED.email,
    genero = EXCLUDED.genero, faixa_etaria = EXCLUDED.faixa_etaria,
    cidade = EXCLUDED.cidade, estado = EXCLUDED.estado, uf = EXCLUDED.uf, pais = EXCLUDED.pais,
    atualizado_em = now();
