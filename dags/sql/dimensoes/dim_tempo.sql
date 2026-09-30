-- Calendário cobrindo todas as datas de venda até a data da carga (idempotente)
INSERT INTO dw.dim_tempo (sk_tempo, data, ano, trimestre, mes, nome_mes, dia,
                          dia_semana, nome_dia_semana, fim_de_semana)
SELECT
    CAST(to_char(d, 'YYYYMMDD') AS INTEGER),
    CAST(d AS DATE),
    EXTRACT(YEAR FROM d), EXTRACT(QUARTER FROM d), EXTRACT(MONTH FROM d),
    (ARRAY['janeiro','fevereiro','março','abril','maio','junho','julho',
           'agosto','setembro','outubro','novembro','dezembro'])[CAST(EXTRACT(MONTH FROM d) AS INTEGER)],
    EXTRACT(DAY FROM d), EXTRACT(ISODOW FROM d),
    (ARRAY['segunda','terça','quarta','quinta','sexta','sábado','domingo'])[CAST(EXTRACT(ISODOW FROM d) AS INTEGER)],
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(
    LEAST(COALESCE((SELECT min(data_venda) FROM shared.vendas_consolidadas), CAST(%(ds)s AS DATE)),
          CAST(%(ds)s AS DATE)),
    CAST(%(ds)s AS DATE),
    INTERVAL '1 day'
) AS d
ON CONFLICT (sk_tempo) DO NOTHING;
