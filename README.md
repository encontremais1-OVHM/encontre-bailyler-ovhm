# encontre-bailyler-ovhm
Aqui você também tem acesso a produtos, fornecedores e compradores... Um espaço pensado para você e o seu negócio não fique de fora. Quem não aparece perde oportunidade de venda e parceria.

---

## Encontre + — WhatsApp Bridge (Baileys)

A integração de WhatsApp para a comunidade Encontre + está em `baileys-service/`.

- Documentação de instalação e API: [baileys-service/README.md](baileys-service/README.md)
- Deploy Railway: configurar Root Directory `/baileys-service` e montar um Volume em `/data`.
- A sessão WhatsApp e os tokens são segredos: nunca os adicionar ao Git.

Este serviço é um bridge não oficial baseado em Baileys. O backend Laravel deve continuar a controlar pagamentos, assinaturas, autorizações e auditoria.
