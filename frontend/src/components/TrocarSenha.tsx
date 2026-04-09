import { useEffect, useState } from "react";
import { authTrocarSenha, setAuth } from "../api/client";
import {
  COLORS,
  SHADOWS,
  RADIUS,
  baseInput,
  baseLabel,
  btnPrimary,
  errorBox,
} from "../styles/theme";
import { Spinner } from "./Spinner";

interface Props {
  username: string;
  obrigatorio?: boolean;
  onSucesso: () => void;
  onCancelar?: () => void;
}

export function TrocarSenha({ username, obrigatorio, onSucesso, onCancelar }: Props) {
  const [senhaAtual, setSenhaAtual] = useState("");
  const [novaSenha, setNovaSenha] = useState("");
  const [confirma, setConfirma] = useState("");
  const [erro, setErro] = useState<string | null>(null);
  const [enviando, setEnviando] = useState(false);

  const onClose = onCancelar;

  // ESC key to close (if not mandatory)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !obrigatorio && onClose) onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [obrigatorio, onClose]);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErro(null);
    if (novaSenha.length < 8) {
      setErro("A nova senha precisa ter ao menos 8 caracteres");
      return;
    }
    if (novaSenha !== confirma) {
      setErro("A confirmacao nao bate com a nova senha");
      return;
    }
    setEnviando(true);
    try {
      await authTrocarSenha(senhaAtual, novaSenha);
      setAuth(username, novaSenha);
      onSucesso();
    } catch (err: unknown) {
      setErro(err instanceof Error ? err.message : "Falha ao trocar senha");
    } finally {
      setEnviando(false);
    }
  }

  const btnSec: React.CSSProperties = {
    ...btnPrimary,
    flex: 1,
    background: COLORS.borderLight,
    color: "#374151",
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,.5)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
      }}
      role="dialog"
      aria-modal="true"
      aria-label={obrigatorio ? "Definir nova senha obrigatoria" : "Trocar senha"}
    >
      <form
        onSubmit={submit}
        style={{
          background: COLORS.bgWhite,
          padding: 32,
          borderRadius: RADIUS.lg,
          width: 420,
          boxShadow: SHADOWS.lg,
        }}
      >
        <h2 style={{ margin: 0, fontSize: 18, color: COLORS.text }}>
          {obrigatorio ? "Defina sua nova senha" : "Trocar senha"}
        </h2>
        {obrigatorio && (
          <p style={{ color: COLORS.warningFg, fontSize: 13, marginTop: 4 }}>
            Sua senha e temporaria. Defina uma senha pessoal antes de continuar.
          </p>
        )}

        <label style={{ ...baseLabel, marginTop: 16 }}>Senha atual</label>
        <input
          type="password"
          value={senhaAtual}
          onChange={(e) => setSenhaAtual(e.target.value)}
          required
          autoComplete="current-password"
          style={baseInput}
          aria-label="Senha atual"
        />

        <label style={{ ...baseLabel, marginTop: 16 }}>Nova senha (min. 8 caracteres)</label>
        <input
          type="password"
          value={novaSenha}
          onChange={(e) => setNovaSenha(e.target.value)}
          required
          minLength={8}
          autoComplete="new-password"
          style={baseInput}
          aria-label="Nova senha"
        />

        <label style={{ ...baseLabel, marginTop: 16 }}>Confirme a nova senha</label>
        <input
          type="password"
          value={confirma}
          onChange={(e) => setConfirma(e.target.value)}
          required
          minLength={8}
          autoComplete="new-password"
          style={baseInput}
          aria-label="Confirmar nova senha"
        />

        {erro && (
          <div style={{ ...errorBox, marginTop: 14 }} role="alert">
            {erro}
          </div>
        )}

        <div style={{ display: "flex", gap: 8, marginTop: 24 }}>
          {!obrigatorio && onCancelar && (
            <button
              type="button"
              onClick={onCancelar}
              style={btnSec}
              aria-label="Cancelar troca de senha"
            >
              Cancelar
            </button>
          )}
          <button
            type="submit"
            disabled={enviando}
            style={{ ...btnPrimary, flex: 1, padding: "10px 20px" }}
            aria-label={enviando ? "Salvando nova senha" : "Salvar nova senha"}
          >
            {enviando ? (
              <>
                <Spinner size={16} color="#ffffff" />
                Salvando...
              </>
            ) : (
              "Salvar"
            )}
          </button>
        </div>
      </form>
    </div>
  );
}
