import { useState } from "react";
import { authTrocarSenha, setAuth } from "../api/client";

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

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErro(null);
    if (novaSenha.length < 8) {
      setErro("A nova senha precisa ter ao menos 8 caracteres");
      return;
    }
    if (novaSenha !== confirma) {
      setErro("A confirmação não bate com a nova senha");
      return;
    }
    setEnviando(true);
    try {
      await authTrocarSenha(senhaAtual, novaSenha);
      // Atualiza credenciais armazenadas com a nova senha
      setAuth(username, novaSenha);
      onSucesso();
    } catch (err: unknown) {
      setErro(err instanceof Error ? err.message : "Falha ao trocar senha");
    } finally {
      setEnviando(false);
    }
  }

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,.4)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
      }}
    >
      <form
        onSubmit={submit}
        style={{
          background: "white",
          padding: 28,
          borderRadius: 12,
          width: 400,
          boxShadow: "0 8px 24px rgba(0,0,0,.2)",
        }}
      >
        <h2 style={{ margin: 0, fontSize: 18 }}>
          {obrigatorio ? "Defina sua nova senha" : "Trocar senha"}
        </h2>
        {obrigatorio && (
          <p style={{ color: "#92400e", fontSize: 13, marginTop: 4 }}>
            Sua senha é temporária. Defina uma senha pessoal antes de continuar.
          </p>
        )}

        <label style={lbl}>Senha atual</label>
        <input
          type="password"
          value={senhaAtual}
          onChange={(e) => setSenhaAtual(e.target.value)}
          required
          autoComplete="current-password"
          style={inp}
        />

        <label style={lbl}>Nova senha (mín. 8 caracteres)</label>
        <input
          type="password"
          value={novaSenha}
          onChange={(e) => setNovaSenha(e.target.value)}
          required
          minLength={8}
          autoComplete="new-password"
          style={inp}
        />

        <label style={lbl}>Confirme a nova senha</label>
        <input
          type="password"
          value={confirma}
          onChange={(e) => setConfirma(e.target.value)}
          required
          minLength={8}
          autoComplete="new-password"
          style={inp}
        />

        {erro && (
          <div
            style={{
              marginTop: 12,
              padding: 10,
              background: "#fecaca",
              color: "#991b1b",
              borderRadius: 6,
              fontSize: 13,
            }}
          >
            {erro}
          </div>
        )}

        <div style={{ display: "flex", gap: 8, marginTop: 20 }}>
          {!obrigatorio && onCancelar && (
            <button type="button" onClick={onCancelar} style={btnSec}>
              Cancelar
            </button>
          )}
          <button type="submit" disabled={enviando} style={btnPri}>
            {enviando ? "Salvando..." : "Salvar"}
          </button>
        </div>
      </form>
    </div>
  );
}

const lbl: React.CSSProperties = {
  display: "block",
  fontSize: 12,
  color: "#5a6c7f",
  marginTop: 12,
  marginBottom: 4,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const inp: React.CSSProperties = {
  width: "100%",
  padding: "10px 12px",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  fontSize: 14,
  boxSizing: "border-box",
};

const btnPri: React.CSSProperties = {
  flex: 1,
  background: "#2563eb",
  color: "white",
  border: 0,
  borderRadius: 6,
  padding: "10px 20px",
  fontSize: 14,
  fontWeight: 600,
  cursor: "pointer",
};

const btnSec: React.CSSProperties = {
  ...btnPri,
  background: "#e5e7eb",
  color: "#374151",
};
