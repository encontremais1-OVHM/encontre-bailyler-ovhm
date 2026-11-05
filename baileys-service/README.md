# Encontre + — Baileys WhatsApp Bridge

Serviço Node.js/TypeScript separado para conectar uma conta WhatsApp via QR Code ou código de vinculação, e expor operações restritas de gestão de grupos para o backend Laravel.

> **Aviso:** Baileys é uma biblioteca não oficial, não é afiliada nem autorizada pelo WhatsApp/Meta. O uso pode estar sujeito a alterações, desconexões ou restrições da conta. Use apenas uma conta controlada pela organização, com autorização dos participantes, sem spam ou mensagens em massa. Para operação comercial crítica, avalie a WhatsApp Business Platform oficial.

## Stack
- Node.js 20+
- TypeScript
- Express
- `@whiskeysockets/baileys` 6.7.24 (versão legacy estável; v7 ainda introduz breaking changes)
- QR Code PNG/Data URL
- Autenticação Bearer por token

## Railway

1. Crie um serviço Railway a partir deste repositório.
2. Defina Root Directory como `/baileys-service`.
3. Use o Dockerfile deste diretório.
4. Adicione um Railway Volume montado em `/data`. **Obrigatório:** a sessão WhatsApp contém chaves criptográficas e deve sobreviver a deploys/restarts.
5. Configure as variáveis abaixo.
6. Gere `API_TOKEN` aleatório com pelo menos 32 caracteres.
7. Faça deploy e valide `GET /health`.

### Variáveis
- `API_TOKEN`: segredo Bearer longo e aleatório (obrigatório).
- `PORT`: fornecido pelo Railway; default 3000.
- `HOST`: default 0.0.0.0.
- `AUTH_DIR`: default /data/baileys-auth.
- `LOG_LEVEL`: default info.
- `CORS_ORIGINS`: origens permitidas, separadas por vírgula. Não exponha este serviço diretamente ao browser; chamadas devem partir do Laravel.
- `WEBHOOK_URL` (opcional): destino para eventos de conexão.
- `WEBHOOK_SECRET` (opcional): segredo HMAC SHA-256 do webhook.

## API

Todas as rotas `/v1/*` exigem `Authorization: Bearer <API_TOKEN>`. `/health` é público para health checks.

- `GET /health`
- `GET /v1/status`
- `POST /v1/connect`
- `GET /v1/qr` — devolve `qr_data_url` quando disponível.
- `POST /v1/pairing-code` — JSON: `{"phone":"2449XXXXXXXX"}` (só dígitos, incluir indicativo).
- `GET /v1/groups`
- `GET /v1/groups/:groupId/participants`
- `POST /v1/groups/:groupId/participants/remove` — JSON: `{"participant":"244...@s.whatsapp.net"}`
- `POST /v1/groups/:groupId/participants/add` — JSON: `{"phone":"2449XXXXXXXX"}`
- `POST /v1/disconnect`
- `POST /v1/logout` — termina sessão e apaga as credenciais persistidas.

Remoção/adição só funciona se a conta conectada tiver permissões de administradora no grupo. O backend deve validar assinatura, período de tolerância e autorização antes de chamar qualquer operação. Nunca exponha este token ao frontend.

## Ligação Laravel

Guardar a URL e token nas variáveis secretas do Laravel/Railway:
- `WHATSAPP_BRIDGE_URL`
- `WHATSAPP_BRIDGE_TOKEN`

O Laravel deve chamar este serviço server-to-server, aplicar idempotência, manter fila/retries, auditar cada operação e confirmar o resultado antes de atualizar `group_memberships`.

## Segurança operacional
- Não guardar QR, pairing code, sessão ou token em logs.
- Não commitar a pasta `/data`, `.env` ou credenciais.
- Limitar acesso de rede ao backend sempre que possível.
- Usar HTTPS na URL pública do Railway.
- Fazer backup seguro e cifrado do volume; limitar acesso ao volume.
- Nunca executar duas réplicas simultâneas usando o mesmo diretório de sessão.
- Rotacionar o token se houver suspeita de exposição.

## Desenvolvimento local
```bash
cp .env.example .env
# definir API_TOKEN com segredo aleatório
npm install
npm run dev
```

## Limitações
Este serviço ainda não sincroniza diretamente com Supabase. O Laravel continua a ser a fonte de verdade de membros, pagamentos, assinaturas e ordens de acesso. O bridge apenas executa comandos WhatsApp autenticados e devolve resultados.
