"""Envio de e-mails de divergência para compradores.

Template: "Sinalizadas - [data D-1]"
Assunto: "Sinalizadas - DD/MM/YYYY"
Corpo: "Bom dia, seguem OC's sinalizadas. Favor retornar com as devidas correções."
Conteúdo: tabela HTML das OCs com divergência agrupada por comprador.

O destinatário é resolvido a partir do `created_by` (ID Club) via
tabela auxiliar `compradores` (gerenciada por `services.compradores`).
"""
from __future__ import annotations

import smtplib
from collections import defaultdict
from datetime import date, datetime
from email.message import EmailMessage

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.config import BASE_DIR, settings
from app.logging_setup import get_logger
from app.models import ResultadoValidacao, StatusValidacao
from app.services import compradores as compradores_svc

logger = get_logger(__name__)

_jinja = Environment(
    loader=FileSystemLoader(BASE_DIR / "templates"),
    autoescape=select_autoescape(["html"]),
)


def agrupar_por_comprador(
    resultados: list[ResultadoValidacao],
) -> dict[int, list[ResultadoValidacao]]:
    """Agrupa as OCs divergentes por created_by (ID do Club).

    Retorna dict de {club_user_id: [resultados]}. OCs sem `created_by`
    caem em chave 0 (grupo "órfãos") e o caller decide o que fazer.
    """
    grupos: dict[int, list[ResultadoValidacao]] = defaultdict(list)
    for r in resultados:
        if r.status == StatusValidacao.APROVADA:
            continue
        key = int(r.oc.created_by) if r.oc.created_by else 0
        grupos[key].append(r)
    return dict(grupos)


def planejar_envios(
    data_d1: date,
    resultados: list[ResultadoValidacao],
) -> tuple[list[dict], list[dict]]:
    """Monta o plano de envios sem enviar nada. Útil para preview na UI.

    Retorna (envios_planejados, ocs_sem_destinatario).
    """
    grupos = agrupar_por_comprador(resultados)
    planejados: list[dict] = []
    orfaos: list[dict] = []
    for club_user_id, ocs in grupos.items():
        nome, email = compradores_svc.resolve(club_user_id or None)
        entry = {
            "club_user_id": club_user_id or None,
            "nome": nome,
            "email": email,
            "qtd_ocs": len(ocs),
            "ocs": [
                {
                    "id_pedido": r.oc.id_pedido,
                    "placa": r.oc.identificador,
                    "fornecedor": r.oc.fornecedor.for_nome if r.oc.fornecedor else None,
                    "motivo": r.motivo_resumido,
                }
                for r in ocs
            ],
        }
        if email:
            planejados.append(entry)
        else:
            orfaos.append(entry)
    return planejados, orfaos


def enviar_notificacoes(
    data_d1: date,
    resultados: list[ResultadoValidacao],
    *,
    force: bool = False,
) -> dict:
    """Envia e-mail a cada comprador com divergências.

    Retorna um dict com estatísticas: enviados, orfaos, erros.
    """
    planejados, orfaos = planejar_envios(data_d1, resultados)
    stats = {
        "enviados": 0,
        "orfaos": len(orfaos),
        "erros": 0,
        "orfaos_detail": orfaos,
    }

    if not planejados:
        logger.info("Nenhuma divergência com destinatário válido para notificar")
        if orfaos:
            logger.warning(
                "%d grupo(s) de divergência sem comprador cadastrado — "
                "use `compradores add` para mapear",
                len(orfaos),
            )
        return stats

    if not settings.email_enabled and not force:
        logger.info(
            "EMAIL_ENABLED=false — %d envio(s) planejado(s) mas não disparados "
            "(use --email ou EMAIL_ENABLED=true)",
            len(planejados),
        )
        return stats

    if not settings.smtp_host or not settings.smtp_user:
        logger.warning("SMTP não configurado — pulando e-mails")
        return stats

    template = _jinja.get_template("email_divergencias.html.j2")
    data_br = data_d1.strftime("%d/%m/%Y")
    gerado_em = datetime.now().strftime("%d/%m/%Y %H:%M")

    grupos = agrupar_por_comprador(resultados)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as smtp:
        smtp.starttls()
        smtp.login(settings.smtp_user, settings.smtp_senha)

        for entry in planejados:
            cuid = entry["club_user_id"]
            nome = entry["nome"]
            destino = entry["email"]
            ocs = grupos.get(cuid or 0, [])

            msg = EmailMessage()
            msg["Subject"] = f"Sinalizadas - {data_br}"
            msg["From"] = settings.smtp_remetente or settings.smtp_user
            msg["To"] = destino
            msg.set_content(
                "Bom dia, seguem OC's sinalizadas. "
                "Favor retornar com as devidas correções."
            )
            msg.add_alternative(
                template.render(
                    comprador=nome,
                    ocs=ocs,
                    data=data_br,
                    gerado_em=gerado_em,
                ),
                subtype="html",
            )
            try:
                smtp.send_message(msg)
                stats["enviados"] += 1
                logger.info("E-mail enviado: %s <%s> -> %d OCs", nome, destino, len(ocs))
            except Exception as e:
                stats["erros"] += 1
                logger.error("Falha ao enviar e-mail para %s: %s", destino, e)

    return stats
