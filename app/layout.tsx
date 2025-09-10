import type { ReactNode } from "react";
import "./globals.css";

export const metadata = {
  title: "Radar Tecnológico — Teknovashop",
  description: "Licitaciones públicas de tecnología, diseño y ciencia.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="es">
      <body className="min-h-full font-sans antialiased">
        <div className="mx-auto max-w-5xl p-4 md:p-8">{children}</div>
      </body>
    </html>
  );
}
