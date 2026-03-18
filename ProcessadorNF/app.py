"""
app.py — Processador de Notas Fiscais
Le PDFs de pastas SMB e envia para a Portal API conforme o perfil configurado.
"""

import configparser
import shutil
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, scrolledtext, simpledialog, ttk

from extractor import extrair_dados
from pdf_reader import extrair_texto
from supabase_client import SupabaseClient

# Credenciais da API — hardcoded, não expostas ao usuário
_API_URL = 'http://187.77.240.80:9000'
_API_KEYS = {
    'portal_municipal_manaus': 'aris-22386d82643aa6e36b12688a7175698d',
    'portal_estado_am':        'estam-52394a9ff66f976ee2ab097a6844a7a4',
}
_PORTAIS = list(_API_KEYS.keys())

# Senha de acesso às configurações
_SENHA_CONFIG = 'tambaqui@'

if getattr(sys, 'frozen', False):
    CONFIG_FILE = Path(sys.executable).parent / 'conf.ini'
else:
    CONFIG_FILE = Path.home() / 'AppData' / 'Roaming' / 'ProcessadorNF' / 'conf.ini'
CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)


def carregar_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE, encoding='utf-8')
    return cfg


def listar_perfis() -> list[dict]:
    cfg = carregar_config()
    perfis = []
    for sec in cfg.sections():
        if sec.startswith('PERFIL_'):
            perfis.append({
                'secao':    sec,
                'nome':     cfg.get(sec, 'nome',     fallback=''),
                'smb_path': cfg.get(sec, 'smb_path', fallback=''),
                'usuario':  cfg.get(sec, 'usuario',  fallback=''),
                'senha':    cfg.get(sec, 'senha',    fallback=''),
                'portal':   cfg.get(sec, 'portal',   fallback='portal_municipal_manaus'),
            })
    return perfis


def salvar_perfil(secao: str, nome: str, smb_path: str, usuario: str, senha: str, portal: str) -> None:
    cfg = carregar_config()
    cfg[secao] = {
        'nome':     nome,
        'smb_path': smb_path,
        'usuario':  usuario,
        'senha':    senha,
        'portal':   portal,
    }
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        cfg.write(f)


def remover_perfil(secao: str) -> None:
    cfg = carregar_config()
    if cfg.has_section(secao):
        cfg.remove_section(secao)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        cfg.write(f)


def proximo_secao() -> str:
    perfis = listar_perfis()
    indices = [int(p['secao'].replace('PERFIL_', '')) for p in perfis if p['secao'].replace('PERFIL_', '').isdigit()]
    return f'PERFIL_{max(indices) + 1}' if indices else 'PERFIL_1'


def autenticar_smb(path: str, usuario: str, senha: str) -> None:
    partes = path.lstrip('\\').split('\\')
    unc = f'\\\\{partes[0]}\\{partes[1]}' if len(partes) >= 2 else path
    subprocess.run(
        ['net', 'use', unc, f'/user:{usuario}', senha],
        capture_output=True, text=True,
    )


def processar_perfil(perfil: dict, log_fn, progress_fn):
    nome    = perfil['nome']
    portal  = perfil['portal']
    api_key = _API_KEYS.get(portal)

    if not api_key:
        log_fn(f'[{nome}] ERRO: portal "{portal}" nao reconhecido.')
        return

    log_fn(f'\n[{nome}] Autenticando na rede...')
    autenticar_smb(perfil['smb_path'], perfil['usuario'], perfil['senha'])

    pasta = Path(perfil['smb_path'])
    if not pasta.exists():
        log_fn(f'[{nome}] ERRO: pasta nao encontrada: {perfil["smb_path"]}')
        return

    pdfs = [p for p in pasta.rglob('*.pdf')
            if not any(x in p.parts for x in ('processados', 'erro'))]

    if not pdfs:
        log_fn(f'[{nome}] Nenhum PDF encontrado.')
        return

    log_fn(f'[{nome}] {len(pdfs)} PDF(s) encontrado(s) → {portal}')
    progress_fn(0, len(pdfs))

    try:
        db = SupabaseClient(_API_URL, api_key, portal)
        db.carregar_numeros_existentes()
        log_fn(f'[{nome}] Cache: {len(db._numeros_cache)} nota(s) existentes.')
    except Exception as e:
        log_fn(f'[{nome}] ERRO ao conectar servidor: {e}')
        return

    pasta_proc = pasta / 'processados'
    pasta_erro = pasta / 'erro'
    pasta_proc.mkdir(exist_ok=True)
    pasta_erro.mkdir(exist_ok=True)

    inseridos = duplicatas = erros = 0

    for i, pdf in enumerate(pdfs, 1):
        progress_fn(i, len(pdfs))
        try:
            texto = extrair_texto(pdf)
            if not texto:
                raise ValueError('PDF vazio ou ilegivel')

            dados = extrair_dados(texto, pdf.name)
            if not dados:
                raise ValueError('Campos obrigatorios nao encontrados')

            inserido = db.inserir_nota(dados)

            if inserido:
                inseridos += 1
                log_fn(f'[{nome}] [{i}/{len(pdfs)}] OK: {pdf.name} | Nota {dados["NumeroNota"]} | R$ {dados["ValorLiquido"]}')
            else:
                duplicatas += 1
                log_fn(f'[{nome}] [{i}/{len(pdfs)}] DUPLICATA: {pdf.name}')

            shutil.move(str(pdf), str(pasta_proc / pdf.name))

        except Exception as e:
            erros += 1
            log_fn(f'[{nome}] [{i}/{len(pdfs)}] ERRO: {pdf.name} — {e}')
            try:
                shutil.move(str(pdf), str(pasta_erro / pdf.name))
            except Exception:
                pass

    log_fn(f'[{nome}] Concluido: {inseridos} inserido(s) | {duplicatas} duplicata(s) | {erros} erro(s)')


def processar(perfis_selecionados, log_fn, progress_fn, btn):
    if not perfis_selecionados:
        log_fn('Nenhum perfil selecionado.')
        btn.config(state=tk.NORMAL)
        return

    for perfil in perfis_selecionados:
        processar_perfil(perfil, log_fn, progress_fn)

    log_fn('\n' + '=' * 50)
    log_fn('Todos os perfis processados.')
    progress_fn(1, 1)
    btn.config(state=tk.NORMAL)


# ── Diálogo de perfil (adicionar/editar) ─────────────────────────────────────

class DialogPerfil(tk.Toplevel):

    def __init__(self, parent, perfil: dict = None):
        super().__init__(parent)
        self.title('Novo Perfil' if perfil is None else 'Editar Perfil')
        self.resizable(False, False)
        self.grab_set()
        self.resultado = None
        self._perfil = perfil

        frame = tk.Frame(self, padx=16, pady=12)
        frame.pack()

        campos = [
            ('Nome do perfil:', 'nome'),
            ('Pasta SMB:',      'smb_path'),
            ('Usuario:',        'usuario'),
            ('Senha SMB:',      'senha'),
        ]
        self._entries = {}
        for row, (label, key) in enumerate(campos):
            tk.Label(frame, text=label, anchor='w', font=('Segoe UI', 9)).grid(row=row, column=0, sticky='w', pady=4)
            show = '*' if key == 'senha' else ''
            entry = tk.Entry(frame, width=42, font=('Segoe UI', 9), show=show)
            entry.grid(row=row, column=1, padx=8, pady=4)
            if perfil:
                entry.insert(0, perfil.get(key, ''))
            self._entries[key] = entry

        tk.Label(frame, text='Portal:', anchor='w', font=('Segoe UI', 9)).grid(row=len(campos), column=0, sticky='w', pady=4)
        self._portal_var = tk.StringVar(value=perfil['portal'] if perfil else _PORTAIS[0])
        portal_cb = ttk.Combobox(frame, textvariable=self._portal_var, values=_PORTAIS,
                                  state='readonly', width=40, font=('Segoe UI', 9))
        portal_cb.grid(row=len(campos), column=1, padx=8, pady=4)

        btn_frame = tk.Frame(self)
        btn_frame.pack(pady=10)
        tk.Button(btn_frame, text='Salvar', font=('Segoe UI', 10, 'bold'),
                  bg='#2563eb', fg='white', relief='flat', padx=16, pady=6,
                  command=self._salvar).pack(side='left', padx=6)
        tk.Button(btn_frame, text='Cancelar', font=('Segoe UI', 10),
                  relief='flat', padx=16, pady=6,
                  command=self.destroy).pack(side='left', padx=6)

    def _salvar(self):
        dados = {k: e.get().strip() for k, e in self._entries.items()}
        dados['portal'] = self._portal_var.get()
        if not all([dados['nome'], dados['smb_path'], dados['usuario'], dados['senha']]):
            messagebox.showwarning('Aviso', 'Preencha todos os campos.', parent=self)
            return
        self.resultado = dados
        self.destroy()


# ── Diálogo de configurações (lista de perfis) ───────────────────────────────

class DialogConfiguracoes(tk.Toplevel):

    def __init__(self, parent):
        super().__init__(parent)
        self.title('Configuracoes — Perfis')
        self.resizable(False, False)
        self.grab_set()
        self._build_ui()
        self._atualizar_lista()

    def _build_ui(self):
        tk.Label(self, text='Perfis configurados', font=('Segoe UI', 11, 'bold')).pack(padx=16, pady=(12, 4))

        frame_lista = tk.Frame(self)
        frame_lista.pack(padx=16, pady=4, fill='both', expand=True)

        self._listbox = tk.Listbox(frame_lista, width=55, height=6, font=('Segoe UI', 9),
                                    selectmode='single', activestyle='dotbox')
        self._listbox.pack(side='left', fill='both', expand=True)

        scroll = tk.Scrollbar(frame_lista, orient='vertical', command=self._listbox.yview)
        scroll.pack(side='right', fill='y')
        self._listbox.config(yscrollcommand=scroll.set)

        btn_frame = tk.Frame(self)
        btn_frame.pack(pady=8)
        tk.Button(btn_frame, text='+ Adicionar', font=('Segoe UI', 9),
                  bg='#16a34a', fg='white', relief='flat', padx=10, pady=4,
                  command=self._adicionar).pack(side='left', padx=4)
        tk.Button(btn_frame, text='✎ Editar', font=('Segoe UI', 9),
                  relief='flat', padx=10, pady=4,
                  command=self._editar).pack(side='left', padx=4)
        tk.Button(btn_frame, text='✕ Remover', font=('Segoe UI', 9),
                  bg='#dc2626', fg='white', relief='flat', padx=10, pady=4,
                  command=self._remover).pack(side='left', padx=4)
        tk.Button(btn_frame, text='Fechar', font=('Segoe UI', 9),
                  relief='flat', padx=10, pady=4,
                  command=self.destroy).pack(side='left', padx=4)

    def _atualizar_lista(self):
        self._perfis = listar_perfis()
        self._listbox.delete(0, 'end')
        for p in self._perfis:
            self._listbox.insert('end', f"{p['nome']}  →  {p['portal']}  |  {p['smb_path']}")

    def _adicionar(self):
        dlg = DialogPerfil(self)
        self.wait_window(dlg)
        if dlg.resultado:
            secao = proximo_secao()
            r = dlg.resultado
            salvar_perfil(secao, r['nome'], r['smb_path'], r['usuario'], r['senha'], r['portal'])
            self._atualizar_lista()

    def _editar(self):
        sel = self._listbox.curselection()
        if not sel:
            messagebox.showwarning('Aviso', 'Selecione um perfil.', parent=self)
            return
        perfil = self._perfis[sel[0]]
        dlg = DialogPerfil(self, perfil=perfil)
        self.wait_window(dlg)
        if dlg.resultado:
            r = dlg.resultado
            salvar_perfil(perfil['secao'], r['nome'], r['smb_path'], r['usuario'], r['senha'], r['portal'])
            self._atualizar_lista()

    def _remover(self):
        sel = self._listbox.curselection()
        if not sel:
            messagebox.showwarning('Aviso', 'Selecione um perfil.', parent=self)
            return
        perfil = self._perfis[sel[0]]
        if messagebox.askyesno('Confirmar', f'Remover perfil "{perfil["nome"]}"?', parent=self):
            remover_perfil(perfil['secao'])
            self._atualizar_lista()


# ── App principal ─────────────────────────────────────────────────────────────

class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title('Processador de Notas Fiscais')
        self.resizable(False, False)
        self._build_ui()
        self._atualizar_perfis()

    def _build_ui(self):
        pad = {'padx': 12, 'pady': 6}

        tk.Label(self, text='Processador de Notas Fiscais',
                 font=('Segoe UI', 13, 'bold')).pack(**pad)

        frame_perfis = tk.LabelFrame(self, text='Perfis', font=('Segoe UI', 9))
        frame_perfis.pack(fill='x', padx=12, pady=4)

        self._perfis_vars = []
        self._frame_checks = tk.Frame(frame_perfis)
        self._frame_checks.pack(fill='x', padx=8, pady=4)

        tk.Label(frame_perfis, text='Nenhum perfil configurado.',
                 font=('Segoe UI', 9), fg='gray').pack(padx=8, anchor='w')
        self._lbl_vazio = frame_perfis.winfo_children()[-1]

        tk.Button(frame_perfis, text='⚙ Configuracoes', font=('Segoe UI', 8),
                  relief='flat', cursor='hand2',
                  command=self._abrir_config).pack(anchor='e', padx=8, pady=4)

        self._progress = ttk.Progressbar(self, length=460, mode='determinate')
        self._progress.pack(**pad)
        self._lbl_progress = tk.Label(self, text='', font=('Segoe UI', 8))
        self._lbl_progress.pack()

        self._btn = tk.Button(
            self, text='Processar PDFs',
            font=('Segoe UI', 11, 'bold'),
            bg='#2563eb', fg='white',
            activebackground='#1d4ed8',
            relief='flat', cursor='hand2',
            padx=20, pady=8,
            command=self._iniciar,
        )
        self._btn.pack(**pad)

        frame_log = tk.LabelFrame(self, text='Log', font=('Segoe UI', 9))
        frame_log.pack(fill='both', expand=True, padx=12, pady=4)
        self._log = scrolledtext.ScrolledText(
            frame_log, height=14, width=64,
            font=('Consolas', 9), state='disabled', wrap='word',
        )
        self._log.pack(padx=4, pady=4)

        tk.Button(self, text='Limpar log', font=('Segoe UI', 8),
                  command=self._limpar_log).pack(pady=(0, 8))

    def _atualizar_perfis(self):
        for w in self._frame_checks.winfo_children():
            w.destroy()
        self._perfis_vars = []

        perfis = listar_perfis()
        if perfis:
            self._lbl_vazio.pack_forget()
            for p in perfis:
                var = tk.BooleanVar(value=True)
                cb = tk.Checkbutton(
                    self._frame_checks,
                    text=f"{p['nome']}  ({p['portal']})",
                    variable=var,
                    font=('Segoe UI', 9),
                    anchor='w',
                )
                cb.pack(fill='x', pady=1)
                self._perfis_vars.append((var, p))
        else:
            self._lbl_vazio.pack(padx=8, anchor='w')

    def _abrir_config(self):
        senha = simpledialog.askstring('Autenticacao', 'Senha:', show='*', parent=self)
        if senha is None:
            return
        if senha != _SENHA_CONFIG:
            messagebox.showerror('Acesso negado', 'Senha incorreta.')
            return
        dlg = DialogConfiguracoes(self)
        self.wait_window(dlg)
        self._atualizar_perfis()

    def _log_write(self, msg: str):
        self._log.config(state='normal')
        self._log.insert('end', msg + '\n')
        self._log.see('end')
        self._log.config(state='disabled')

    def _limpar_log(self):
        self._log.config(state='normal')
        self._log.delete('1.0', 'end')
        self._log.config(state='disabled')

    def _atualizar_progress(self, atual: int, total: int):
        self._progress['maximum'] = total
        self._progress['value']   = atual
        self._lbl_progress.config(text=f'{atual} / {total}')

    def _iniciar(self):
        perfis_selecionados = [p for var, p in self._perfis_vars if var.get()]
        if not perfis_selecionados:
            messagebox.showwarning('Aviso', 'Selecione ao menos um perfil.')
            return
        self._btn.config(state=tk.DISABLED)
        self._limpar_log()
        self._progress['value'] = 0
        self._lbl_progress.config(text='')
        threading.Thread(
            target=processar,
            args=(perfis_selecionados, self._log_write, self._atualizar_progress, self._btn),
            daemon=True,
        ).start()


if __name__ == '__main__':
    app = App()
    app.mainloop()
