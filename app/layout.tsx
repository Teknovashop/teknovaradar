import type { ReactNode } from "react";

export const metadata = {
  title: "Radar Tecnológico — Teknovashop",
  description: "Licitaciones públicas de tecnología, diseño y ciencia.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="es">
      <body>{children}</body>
    </html>
  );
}
