# =============================================================================
# Padrões de sparklyr no Databricks: limpeza, pivot, unpivot e cruzamentos
# -----------------------------------------------------------------------------
# ATENÇÃO: só bd_bronze.base_credito_atual veio do seu exemplo. As demais
# tabelas e colunas são NOMES FICTÍCIOS para ilustrar cada padrão; troque
# pelos seus. Layout assumido:
#
#   base_credito_atual   : num_conta, num_cpf_cnpj, vlr_saldo
#   base_investimentos   : num_cpf_cnpj, dsc_produto, vlr_saldo        (longa)
#   base_saldos_mensal   : num_conta, dt_referencia, vlr_saldo         (longa)
#   base_produtos_wide   : num_cpf_cnpj, vlr_cartao, vlr_consorcio,
#                          vlr_seguro                                  (larga)
#   base_cadastro        : num_cpf_cnpj, nom_cliente, cod_agencia,
#                          dt_atualizacao
#   base_contas          : num_conta, num_cpf_cnpj
# =============================================================================

library(sparklyr)
library(dplyr)
library(tidyr)

sc <- spark_connect(method = "databricks")


# =============================================================================
# 0. FUNÇÕES AUXILIARES (reaproveite em todos os notebooks)
# =============================================================================

# Lê uma tabela Delta de forma "lazy" (nada é carregado no R)
ler <- function(tabela, schema = "bd_bronze") {
  tbl(sc, dbplyr::in_schema(schema, tabela))
}

# Trim em todas as colunas de texto (substitui across(where(is.character), trimws))
trim_texto <- function(df) {
  cols <- sdf_schema(df) |>
    purrr::keep(\(col) col$type == "StringType") |>
    names()
  df |> mutate(across(all_of(cols), trim))
}

# Remove ponto, barra e hífen de CPF/CNPJ
limpa_doc <- function(df, col = num_cpf_cnpj) {
  df |> mutate({{ col }} := regexp_replace(trim({{ col }}), "[./-]", ""))
}

# Remove hífen e completa a conta com zeros à esquerda.
# Não usa lpad direto em tudo porque o lpad do Spark TRUNCA valores maiores
# que a largura (o str_pad do R não trunca).
limpa_conta <- function(df, col = num_conta, largura = 6L) {
  df |>
    mutate({{ col }} := regexp_replace(trim({{ col }}), "-", "")) |>
    mutate({{ col }} := if_else(char_length({{ col }}) < largura,
                                lpad({{ col }}, largura, "0"),
                                {{ col }}))
}

# Padroniza um texto para virar nome de coluna (sem espaço, acento ou símbolo).
# Necessário antes de pivotar: o valor da categoria vira nome de coluna, e
# tabela Delta não aceita espaço/caracter especial em nome de coluna.
limpa_categoria <- function(df, col) {
  df |>
    mutate({{ col }} := regexp_replace(lower(trim({{ col }})), "[^a-z0-9]+", "_"))
}

# Troca NA por 0 em todas as colunas, exceto as chaves
zera_na <- function(df, chaves) {
  df |> mutate(across(-all_of(chaves), \(x) coalesce(x, 0)))
}

# Confere se a chave é única (essencial antes de qualquer join)
checa_chave <- function(df, ...) {
  df |> count(...) |> filter(n > 1) |> sdf_nrow()
}


# =============================================================================
# 1. BASE SIMPLES: limpeza + agregação (seu exemplo)
# =============================================================================

credito <- ler("base_credito_atual") |>
  select(num_conta, num_cpf_cnpj, vlr_saldo) |>
  limpa_doc() |>
  limpa_conta() |>
  group_by(num_conta, num_cpf_cnpj) |>
  summarise(vlr_credito = sum(vlr_saldo, na.rm = TRUE), .groups = "drop")

# Visão por cliente (para cruzar com bases que estão no nível de CPF/CNPJ)
credito_cliente <- credito |>
  group_by(num_cpf_cnpj) |>
  summarise(
    qtd_contas_credito = n_distinct(num_conta),
    vlr_credito        = sum(vlr_credito, na.rm = TRUE),
    .groups = "drop"
  )


# =============================================================================
# 2. PIVOT (longa -> larga): três variações
# =============================================================================

invest <- ler("base_investimentos") |>
  select(num_cpf_cnpj, dsc_produto, vlr_saldo) |>
  limpa_doc() |>
  limpa_categoria(dsc_produto)

# --- 2A. sdf_pivot: categorias descobertas automaticamente -------------------
# Bom quando você não sabe (ou não quer fixar) a lista de produtos.
# As colunas saem com o nome de cada produto.
invest_pivot_a <- invest |>
  sdf_pivot(num_cpf_cnpj ~ dsc_produto,
            fun.aggregate = list(vlr_saldo = "sum")) |>
  zera_na("num_cpf_cnpj")

# --- 2B. Agregação condicional: categorias fixas -----------------------------
# Mais rápida e com colunas estáveis (o layout não muda se surgir produto
# novo). Preferível em tabelas que alimentam painel/modelo.
invest_pivot_b <- invest |>
  group_by(num_cpf_cnpj) |>
  summarise(
    vlr_cdb      = sum(if_else(dsc_produto == "cdb",      vlr_saldo, 0), na.rm = TRUE),
    vlr_lci      = sum(if_else(dsc_produto == "lci",      vlr_saldo, 0), na.rm = TRUE),
    vlr_poupanca = sum(if_else(dsc_produto == "poupanca", vlr_saldo, 0), na.rm = TRUE),
    vlr_invest_total = sum(vlr_saldo, na.rm = TRUE),
    .groups = "drop"
  )

# --- 2C. pivot_wider (sintaxe tidyr) -----------------------------------------
# Agrega ANTES para garantir uma linha por chave + categoria.
invest_pivot_c <- invest |>
  group_by(num_cpf_cnpj, dsc_produto) |>
  summarise(vlr_saldo = sum(vlr_saldo, na.rm = TRUE), .groups = "drop") |>
  pivot_wider(
    names_from   = dsc_produto,
    values_from  = vlr_saldo,
    names_prefix = "vlr_",
    values_fill  = 0
  )

# --- 2D. Pivot por mês (série temporal em colunas) ---------------------------
saldos_mes <- ler("base_saldos_mensal") |>
  select(num_conta, dt_referencia, vlr_saldo) |>
  limpa_conta() |>
  filter(dt_referencia >= "2026-01-01") |>           # filtre ANTES de pivotar
  mutate(mes = date_format(dt_referencia, "yyyyMM")) |>
  group_by(num_conta, mes) |>
  summarise(vlr_saldo = sum(vlr_saldo, na.rm = TRUE), .groups = "drop") |>
  pivot_wider(
    names_from   = mes,
    values_from  = vlr_saldo,
    names_prefix = "saldo_",
    values_fill  = 0
  )


# =============================================================================
# 3. UNPIVOT (larga -> longa)
# =============================================================================

produtos_wide <- ler("base_produtos_wide") |>
  limpa_doc()

# --- 3A. pivot_longer --------------------------------------------------------
produtos_long <- produtos_wide |>
  pivot_longer(
    cols         = starts_with("vlr_"),
    names_to     = "dsc_produto",
    names_prefix = "vlr_",
    values_to    = "vlr_saldo"
  ) |>
  filter(!is.na(vlr_saldo), vlr_saldo != 0)

# --- 3B. Alternativa em SQL (stack), caso o pivot_longer não atenda ----------
produtos_long_sql <- sdf_sql(sc, "
  select num_cpf_cnpj,
         stack(3,
               'cartao',    vlr_cartao,
               'consorcio', vlr_consorcio,
               'seguro',    vlr_seguro) as (dsc_produto, vlr_saldo)
  from bd_bronze.base_produtos_wide
")

# Depois de "alongar", dá para reagregar e pivotar de outro jeito
produtos_cliente <- produtos_long |>
  group_by(num_cpf_cnpj) |>
  summarise(
    qtd_produtos = n_distinct(dsc_produto),
    vlr_produtos = sum(vlr_saldo, na.rm = TRUE),
    .groups = "drop"
  )


# =============================================================================
# 4. PREPARO DAS DIMENSÕES (deixar uma linha por chave antes de cruzar)
# =============================================================================

# --- 4A. Registro mais recente por cliente (deduplicação com janela) ---------
cadastro <- ler("base_cadastro") |>
  select(num_cpf_cnpj, nom_cliente, cod_agencia, dt_atualizacao) |>
  trim_texto() |>
  limpa_doc() |>
  group_by(num_cpf_cnpj) |>
  window_order(desc(dt_atualizacao)) |>
  filter(row_number() == 1) |>
  ungroup() |>
  select(-dt_atualizacao)

# --- 4B. Tabela pequena que está no R (de-para) -> sobe para o Spark ---------
de_para_agencia <- data.frame(
  cod_agencia = c(1L, 2L, 3L),
  nom_regiao  = c("Norte", "Sul", "Centro")
)
agencias <- copy_to(sc, de_para_agencia, "tmp_de_para_agencia", overwrite = TRUE)

# --- 4C. Conferência de chave única ------------------------------------------
# Deve retornar 0. Se não retornar, o join vai multiplicar linhas e valores.
checa_chave(cadastro, num_cpf_cnpj)
checa_chave(invest_pivot_b, num_cpf_cnpj)
checa_chave(credito_cliente, num_cpf_cnpj)


# =============================================================================
# 5. CRUZAMENTOS
# =============================================================================

# --- 5A. Visão cliente: left joins em cadeia ---------------------------------
visao_cliente <- cadastro |>
  left_join(credito_cliente,  by = "num_cpf_cnpj") |>
  left_join(invest_pivot_b,   by = "num_cpf_cnpj") |>
  left_join(produtos_cliente, by = "num_cpf_cnpj") |>
  left_join(sdf_broadcast(agencias), by = "cod_agencia") |>   # tabela pequena
  mutate(
    across(c(starts_with("vlr_"), starts_with("qtd_")), \(x) coalesce(x, 0)),
    vlr_relacionamento = vlr_credito + vlr_invest_total + vlr_produtos,
    flg_tem_credito    = if_else(vlr_credito > 0, 1L, 0L)
  )

# --- 5B. Full join: clientes que estão em uma base OU na outra ---------------
credito_x_invest <- credito_cliente |>
  full_join(invest_pivot_b, by = "num_cpf_cnpj") |>
  zera_na("num_cpf_cnpj")

# --- 5C. Anti join: quem está em uma base e NÃO está na outra ----------------
credito_sem_cadastro <- credito_cliente |>
  anti_join(cadastro, by = "num_cpf_cnpj")

# --- 5D. Semi join: filtra pela existência, sem trazer colunas ---------------
cadastro_com_credito <- cadastro |>
  semi_join(credito_cliente, by = "num_cpf_cnpj")

# --- 5E. Chaves com nomes diferentes e chave composta ------------------------
contas <- ler("base_contas") |>
  select(conta = num_conta, doc = num_cpf_cnpj) |>
  limpa_conta(conta) |>
  limpa_doc(doc)

credito_contas <- credito |>
  inner_join(contas, by = c("num_conta" = "conta", "num_cpf_cnpj" = "doc"))

# --- 5F. Cruzar a série mensal pivotada com o crédito por conta --------------
conta_completa <- credito |>
  left_join(saldos_mes, by = "num_conta") |>
  zera_na(c("num_conta", "num_cpf_cnpj"))

# --- 5G. Agregar depois do cruzamento (ex.: por agência/região) --------------
resumo_regiao <- visao_cliente |>
  group_by(cod_agencia, nom_regiao) |>
  summarise(
    qtd_clientes     = n(),
    qtd_com_credito  = sum(flg_tem_credito, na.rm = TRUE),
    vlr_credito      = sum(vlr_credito, na.rm = TRUE),
    vlr_invest_total = sum(vlr_invest_total, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(pct_com_credito = qtd_com_credito / qtd_clientes)


# =============================================================================
# 6. VALIDAÇÃO, CACHE E GRAVAÇÃO
# =============================================================================

# Resultado usado várias vezes: materializa uma vez para não recalcular tudo
visao_cliente <- visao_cliente |> compute("tmp_visao_cliente")

# O cruzamento não pode aumentar o número de linhas da base principal
sdf_nrow(cadastro)
sdf_nrow(visao_cliente)

# Ver o SQL gerado / o plano de execução
show_query(resumo_regiao)

# Gravar como tabela Delta
spark_write_table(visao_cliente, "bd_silver.visao_cliente", mode = "overwrite")
spark_write_table(resumo_regiao, "bd_silver.resumo_regiao", mode = "overwrite")

# Trazer para o R só o que é pequeno (resultado agregado)
resumo_local <- collect(resumo_regiao)
