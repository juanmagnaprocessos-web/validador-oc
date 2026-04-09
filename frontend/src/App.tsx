import { useState } from "react";
import { useAuth } from "./contexts/AuthContext";
import { AdminUsers } from "./components/AdminUsers";
import { Dashboard } from "./components/Dashboard";
import { Login } from "./components/Login";
import { Shell } from "./components/Shell";
import { TrocarSenha } from "./components/TrocarSenha";
import { Spinner } from "./components/Spinner";

type Tela = "dashboard" | "admin";

export default function App() {
  const { user, loading, setUser } = useAuth();
  const [tela, setTela] = useState<Tela>("dashboard");
  const [trocandoSenha, setTrocandoSenha] = useState(false);

  if (loading) {
    return (
      <div
        style={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <Spinner size={40} label="Carregando sessao..." />
      </div>
    );
  }

  if (!user) {
    return <Login />;
  }

  const forcarTroca = user.must_change_password;

  return (
    <>
      {forcarTroca && (
        <TrocarSenha
          username={user.username}
          obrigatorio
          onSucesso={() => setUser({ ...user, must_change_password: false })}
        />
      )}

      {trocandoSenha && !forcarTroca && (
        <TrocarSenha
          username={user.username}
          onSucesso={() => setTrocandoSenha(false)}
          onCancelar={() => setTrocandoSenha(false)}
        />
      )}

      <Shell
        tela={tela}
        onTela={setTela}
        onTrocarSenha={() => setTrocandoSenha(true)}
      >
        {tela === "dashboard" ? <Dashboard /> : <AdminUsers />}
      </Shell>
    </>
  );
}
