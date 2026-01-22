# Configurações centralizadas do Conciliador de Deslocamentos
# =============================================================
# Este arquivo contém todas as constantes configuráveis do sistema.
# Altere os valores conforme necessário para ajustar o comportamento.

# ==========================================
# CLASSIFICAÇÃO DE DESLOCAMENTOS (V5)
# ==========================================

# Velocidade mínima para considerar em movimento (km/h)
VELOCIDADE_MOVIMENTO = 3

# Duração mínima para um período ser válido (minutos)
# Períodos menores são absorvidos pelo período anterior
MIN_DURACAO_PERIODO = 5

# Gap máximo de parada que não fragmenta um movimento (minutos)
# Paradas menores que isso são consideradas parte do movimento
GAP_CONSOLIDACAO = 15

# Tempo mínimo desde o último ponto para considerar período "fechado" (minutos)
# Períodos mais recentes são considerados "em curso" e não são inseridos
TEMPO_PERIODO_EM_CURSO = 30

# ==========================================
# CONFIGURAÇÕES LEGADAS (V4) - Mantidas para referência
# ==========================================

# Tempo de gap de sinal para considerar nova viagem (minutos)
GAP_THRESHOLD_MINUTES = 20

# Velocidade abaixo da qual considera parado (km/h)
STOP_THRESHOLD_KMH = 3

# Viagens menores que isso são descartadas como ruído/manobra (km)
MIN_DISTANCIA_VIAGEM = 2

# Gap maior que isso indica provável perda de sinal GPS (minutos)
SIGNAL_LOSS_THRESHOLD = 60

# Velocidade máxima realista para filtrar dados incorretos (km/h)
MAX_SPEED_REALISTIC = 150

# Tempo com ignição OFF para fechar deslocamento (minutos)
TEMPO_IGN_OFF_PARADA = 10

# Distância mínima para reiniciar deslocamento após parada (km)
DIST_REINICIO_DESLOCAMENTO = 3

# ==========================================
# GEOCODIFICAÇÃO
# ==========================================

# Raio para arredondamento de coordenadas (casas decimais)
# 3 casas = aprox. 100m de precisão
PRECISAO_COORDENADAS = 3

# Timeout para chamadas de geocodificação reversa (segundos)
GEOCODING_TIMEOUT = 3

# ==========================================
# PROCESSAMENTO EM BATCH
# ==========================================

# Tamanho do lote para inserções no banco
BATCH_SIZE = 50

# ==========================================
# CONSOLIDAÇÃO PÓS-PROCESSAMENTO
# ==========================================

# Tolerância de gap para consolidar períodos consecutivos (minutos)
TOLERANCIA_CONSOLIDACAO = 30
