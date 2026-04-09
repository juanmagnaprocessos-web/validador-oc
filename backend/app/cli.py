"""CLI do validador-oc.

Uso:
    python -m app.cli validar --data 2026-04-05 --dry-run
    python -m app.cli validar --data 2026-04-05 --apply
    python -m app.cli historico
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta

from rich.console import Console
from rich.table import Table

from app.db import listar_historico
from app.logging_setup import setup_logging
from app.services import compradores as compradores_svc
from app.services.emailer import enviar_notificacoes, planejar_envios
from app.services.orchestrator import executar_validacao
from app.services.report import gerar_excel, gerar_html

console = Console()


def _d1_default() -> date:
    return date.today() - timedelta(days=1)


def _parse_data(s: str) -> date:
    return date.fromisoformat(s)


async def cmd_validar(args: argparse.Namespace) -> int:
    data_d1 = _parse_data(args.data) if args.data else _d1_default()
    dry_run = not args.apply

    console.print(
        f"[cyan]Validando OCs de [bold]{data_d1}[/bold]"
        f" ({'DRY-RUN' if dry_run else 'APLICANDO'})...[/cyan]"
    )

    try:
        validacao_id, resultados, ocs_orfas = await executar_validacao(
            data_d1, dry_run=dry_run
        )
    except Exception as e:
        console.print(f"[red bold]ERRO:[/red bold] {e}")
        raise

    # Relatórios
    html_path = gerar_html(data_d1, resultados, dry_run=dry_run, ocs_orfas=ocs_orfas)
    xlsx_path = gerar_excel(data_d1, resultados, ocs_orfas=ocs_orfas)

    # E-mails
    if args.email:
        stats = enviar_notificacoes(data_d1, resultados, force=True)
        console.print(
            f"[green]E-mails:[/green] enviados={stats['enviados']} "
            f"orfaos={stats['orfaos']} erros={stats['erros']}"
        )
        if stats["orfaos"]:
            console.print(
                "[yellow]Aviso:[/yellow] há divergências sem comprador "
                "cadastrado. Use [bold]compradores add[/bold] para mapear."
            )
    else:
        # Sempre mostrar preview do que SERIA enviado
        planejados, orfaos = planejar_envios(data_d1, resultados)
        if planejados or orfaos:
            console.print(
                f"\n[dim]Preview de e-mails: {len(planejados)} pronto(s), "
                f"{len(orfaos)} sem destinatário (use --email para enviar).[/dim]"
            )

    # Resumo visual
    total = len(resultados)
    aprovadas = sum(1 for r in resultados if r.aprovada)
    divergentes = total - aprovadas

    tbl = Table(title=f"Validação #{validacao_id} — {data_d1.isoformat()}")
    tbl.add_column("Placa")
    tbl.add_column("Fornecedor")
    tbl.add_column("Valor Club", justify="right")
    tbl.add_column("Cot.", justify="right")
    tbl.add_column("Status")
    tbl.add_column("Motivo")

    for r in resultados:
        forn = (r.oc.fornecedor.for_nome or "") if r.oc.fornecedor else ""
        status_color = {
            "aprovada": "[green]✅ Aprovada[/green]",
            "divergencia": "[yellow]⚠️ Divergência[/yellow]",
            "bloqueada": "[red]❌ Bloqueada[/red]",
        }[r.status.value]
        tbl.add_row(
            r.oc.identificador or "—",
            (forn[:24] + "…") if len(forn) > 25 else forn,
            f"R$ {r.valor_club:.2f}" if r.valor_club else "—",
            str(r.qtd_cotacoes or 0),
            status_color,
            (r.motivo_resumido[:40] + "…")
            if len(r.motivo_resumido) > 41
            else r.motivo_resumido,
        )

    console.print(tbl)
    console.print(
        f"\n[bold]{total}[/bold] OCs → "
        f"[green]{aprovadas} ✅[/green] / [yellow]{divergentes} ⚠️[/yellow]"
    )
    console.print(f"[blue]HTML:[/blue] {html_path}")
    console.print(f"[blue]Excel:[/blue] {xlsx_path}")
    return 0


def cmd_compradores(args: argparse.Namespace) -> int:
    acao = args.acao
    if acao == "list":
        regs = compradores_svc.listar()
        if not regs:
            console.print("[yellow]Tabela vazia.[/yellow] Use 'compradores add' para cadastrar.")
            return 0
        tbl = Table(title=f"Compradores cadastrados ({len(regs)})")
        tbl.add_column("club_user_id", justify="right")
        tbl.add_column("Nome")
        tbl.add_column("E-mail")
        tbl.add_column("Ativo")
        for r in regs:
            tbl.add_row(
                str(r["club_user_id"]),
                r["nome"],
                r["email"],
                "sim" if r["ativo"] else "não",
            )
        console.print(tbl)
        return 0

    if acao == "add":
        compradores_svc.add(args.club_user_id, args.nome, args.email)
        console.print(
            f"[green]OK[/green] comprador {args.club_user_id} "
            f"({args.nome} <{args.email}>) cadastrado."
        )
        return 0

    if acao == "remove":
        ok = compradores_svc.remove(args.club_user_id)
        if ok:
            console.print(f"[green]OK[/green] removido {args.club_user_id}.")
        else:
            console.print(f"[yellow]Não encontrado:[/yellow] {args.club_user_id}")
        return 0 if ok else 1

    if acao == "init":
        # Descobre IDs distintos nas OCs recentes e popula com placeholders
        import asyncio as _a
        from datetime import date as _d, timedelta as _td

        async def _scan():
            from app.clients.club_client import ClubClient

            async with ClubClient() as c:
                ids: dict[int, int] = {}
                for delta in range(1, args.dias + 1):
                    d = _d.today() - _td(days=delta)
                    for o in await c.listar_pedidos(d):
                        cb = o.get("created_by")
                        if cb:
                            ids[int(cb)] = ids.get(int(cb), 0) + 1
                return ids

        ids = _a.run(_scan())
        if not ids:
            console.print("[yellow]Nenhum created_by encontrado nas OCs recentes.[/yellow]")
            return 0

        existentes = {r["club_user_id"] for r in compradores_svc.listar()}
        novos = sorted(set(ids) - existentes, key=lambda x: -ids[x])
        console.print(
            f"Encontrados [bold]{len(ids)}[/bold] ID(s) distintos. "
            f"Já cadastrados: {len(ids) - len(novos)}. "
            f"Novos: {len(novos)}."
        )
        for cid in novos:
            console.print(
                f"  [cyan]{cid}[/cyan] ({ids[cid]} OCs) — use "
                f"[bold]compradores add {cid} \"Nome Completo\" email@magnaprotecao.com.br[/bold]"
            )
        return 0

    return 1


def cmd_criar_admin(args: argparse.Namespace) -> int:
    """Bootstrap do primeiro usuário Admin do sistema.

    Cria o perfil 'Admin' (com permissão '*') se não existir e cria
    um usuário com `must_change_password=True`.
    """
    from app.db import (
        criar_perfil,
        criar_usuario,
        get_perfil_por_nome,
        get_usuario_por_username,
        init_db,
    )
    from app.services.auth import gerar_senha_temporaria, hash_senha

    init_db()

    if get_usuario_por_username(args.username):
        console.print(f"[red]Já existe usuário '{args.username}'.[/red]")
        return 1

    perfil = get_perfil_por_nome("Admin")
    if perfil:
        perfil_id = perfil["id"]
        console.print(f"[dim]Perfil 'Admin' já existe (id={perfil_id}).[/dim]")
    else:
        perfil_id = criar_perfil(
            "Admin",
            "Acesso total ao sistema",
            ["*"],
        )
        console.print(f"[green]Perfil 'Admin' criado (id={perfil_id}).[/green]")

    senha = args.senha or gerar_senha_temporaria()
    if len(senha) < 8:
        console.print("[red]Senha precisa ter no mínimo 8 caracteres.[/red]")
        return 1

    novo_id = criar_usuario(
        username=args.username,
        nome=args.nome,
        email=args.email,
        senha_hash=hash_senha(senha),
        perfil_id=perfil_id,
        must_change_password=True,
    )

    console.print(
        f"\n[green bold]Usuario Admin criado com sucesso![/green bold]\n"
        f"  id:        {novo_id}\n"
        f"  username:  [cyan]{args.username}[/cyan]\n"
        f"  nome:      {args.nome}\n"
        f"  senha:     [yellow]{senha}[/yellow]  (guarde! sera exigida troca no 1o login)\n"
    )
    return 0


def cmd_historico(args: argparse.Namespace) -> int:
    hist = listar_historico(args.limite)
    tbl = Table(title="Histórico de validações")
    tbl.add_column("ID", justify="right")
    tbl.add_column("Execução")
    tbl.add_column("D-1")
    tbl.add_column("Total", justify="right")
    tbl.add_column("✅", justify="right")
    tbl.add_column("⚠️", justify="right")
    tbl.add_column("Dry")
    tbl.add_column("Status")
    for h in hist:
        tbl.add_row(
            str(h["id"]),
            h["data_execucao"],
            h["data_d1"],
            str(h["total_ocs"]),
            str(h["aprovadas"]),
            str(h["divergentes"]),
            "sim" if h["dry_run"] else "não",
            h["status"],
        )
    console.print(tbl)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="validador-oc",
        description="Validação diária de Ordens de Compra",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validar", help="Valida OCs do D-1")
    v.add_argument("--data", help="Data D-1 (YYYY-MM-DD). Default: ontem.")
    grp = v.add_mutually_exclusive_group()
    grp.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Não altera Pipefy (default).",
    )
    grp.add_argument(
        "--apply",
        action="store_true",
        help="Aplica mudanças no Pipefy de verdade.",
    )
    v.add_argument(
        "--email", action="store_true", help="Envia e-mails de divergência."
    )

    sub.add_parser("historico", help="Lista últimas validações").add_argument(
        "--limite", type=int, default=20
    )

    # criar-admin
    ca = sub.add_parser("criar-admin", help="Cria o primeiro usuário Admin")
    ca.add_argument("--username", required=True)
    ca.add_argument("--nome", required=True)
    ca.add_argument("--email", default=None)
    ca.add_argument("--senha", default=None, help="Se omitida, gera uma temporária")

    # compradores
    comp = sub.add_parser("compradores", help="Gerencia a tabela de compradores")
    comp_sub = comp.add_subparsers(dest="acao", required=True)
    comp_sub.add_parser("list", help="Lista compradores cadastrados")
    comp_add = comp_sub.add_parser("add", help="Cadastra/atualiza um comprador")
    comp_add.add_argument("club_user_id", type=int, help="ID no Club (campo created_by)")
    comp_add.add_argument("nome", help="Nome completo")
    comp_add.add_argument("email", help="E-mail")
    comp_rm = comp_sub.add_parser("remove", help="Remove um comprador")
    comp_rm.add_argument("club_user_id", type=int)
    comp_init = comp_sub.add_parser(
        "init", help="Descobre IDs nas OCs recentes e lista os que faltam cadastrar"
    )
    comp_init.add_argument("--dias", type=int, default=30)

    return p


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    args = build_parser().parse_args(argv)

    if args.cmd == "validar":
        return asyncio.run(cmd_validar(args))
    if args.cmd == "historico":
        return cmd_historico(args)
    if args.cmd == "compradores":
        return cmd_compradores(args)
    if args.cmd == "criar-admin":
        return cmd_criar_admin(args)

    return 1


if __name__ == "__main__":
    sys.exit(main())
