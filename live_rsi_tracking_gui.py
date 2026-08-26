#!/usr/bin/env python
from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk


BASE_DIR = Path(__file__).resolve().parent
TRACKER_SCRIPT = BASE_DIR / "live_rsi_tracking.py"


class LiveRsiTrackingGui:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Live RSI Trading Console")
        self.root.geometry("1060x760")
        self.root.minsize(920, 640)

        self.process: subprocess.Popen[str] | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()

        self.interval_var = tk.StringVar(value="30")
        self.symbols_var = tk.StringVar(value="")
        self.buy_rsi_protection_var = tk.StringVar(value="1.0")
        self.min_profit_pct_var = tk.StringVar(value="0.0")
        self.input_var = tk.StringVar(value="")

        self.hybrid_var = tk.BooleanVar(value=True)
        self.telegram_var = tk.BooleanVar(value=True)
        self.confirm_order_var = tk.BooleanVar(value=True)
        self.dry_run_var = tk.BooleanVar(value=False)
        self.upstox_live_var = tk.BooleanVar(value=True)
        self.sync_daily_data_var = tk.BooleanVar(value=True)
        self.results_var = tk.BooleanVar(value=False)

        self._configure_style()
        self._build_ui()
        self._set_running_state(False)

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(120, self._drain_log_queue)

    def _configure_style(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        bg = "#121826"
        card = "#1B2435"
        fg = "#E8EEF9"
        muted = "#AAB8D6"
        accent = "#3C8DFF"

        self.root.configure(bg=bg)

        style.configure("App.TFrame", background=bg)
        style.configure("Card.TFrame", background=card)
        style.configure("Card.TLabelframe", background=card, foreground=fg, borderwidth=1)
        style.configure("Card.TLabelframe.Label", background=card, foreground=fg)
        style.configure("App.TLabel", background=bg, foreground=fg)
        style.configure("Muted.TLabel", background=bg, foreground=muted)
        style.configure("Card.TLabel", background=card, foreground=fg)
        style.configure("Accent.TButton", padding=(12, 6), foreground="#FFFFFF", background=accent)
        style.map("Accent.TButton", background=[("active", "#2878EB")])

    def _build_ui(self) -> None:
        shell = ttk.Frame(self.root, style="App.TFrame", padding=14)
        shell.pack(fill="both", expand=True)

        header = ttk.Frame(shell, style="App.TFrame")
        header.pack(fill="x", pady=(0, 10))

        ttk.Label(header, text="Live RSI Trading Console", style="App.TLabel", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        ttk.Label(
            header,
            text="Launch, monitor, and interact with live_rsi_tracking.py",
            style="Muted.TLabel",
            font=("Segoe UI", 10),
        ).pack(anchor="w", pady=(2, 0))

        config_card = ttk.LabelFrame(shell, text="Run Configuration", style="Card.TLabelframe", padding=12)
        config_card.pack(fill="x")

        row1 = ttk.Frame(config_card, style="Card.TFrame")
        row1.pack(fill="x", pady=4)
        ttk.Label(row1, text="Interval (sec)", style="Card.TLabel").pack(side="left")
        ttk.Entry(row1, textvariable=self.interval_var, width=10).pack(side="left", padx=(8, 22))
        ttk.Label(row1, text="Symbols (space separated)", style="Card.TLabel").pack(side="left")
        ttk.Entry(row1, textvariable=self.symbols_var).pack(side="left", padx=8, fill="x", expand=True)

        row2 = ttk.Frame(config_card, style="Card.TFrame")
        row2.pack(fill="x", pady=4)
        ttk.Label(row2, text="Buy RSI Protection", style="Card.TLabel").pack(side="left")
        ttk.Entry(row2, textvariable=self.buy_rsi_protection_var, width=10).pack(side="left", padx=(8, 22))
        ttk.Label(row2, text="Min Profit %", style="Card.TLabel").pack(side="left")
        ttk.Entry(row2, textvariable=self.min_profit_pct_var, width=10).pack(side="left", padx=(8, 22))

        flags_frame = ttk.Frame(config_card, style="Card.TFrame")
        flags_frame.pack(fill="x", pady=(6, 0))

        options = [
            ("Hybrid", self.hybrid_var),
            ("Telegram", self.telegram_var),
            ("Confirm Order", self.confirm_order_var),
            ("Dry Run", self.dry_run_var),
            ("Upstox Live", self.upstox_live_var),
            ("Auto Sync Daily Data", self.sync_daily_data_var),
            ("Results Only", self.results_var),
        ]

        for index, (label, var) in enumerate(options):
            ttk.Checkbutton(flags_frame, text=label, variable=var).grid(
                row=index // 4,
                column=index % 4,
                padx=8,
                pady=4,
                sticky="w",
            )

        controls = ttk.Frame(shell, style="App.TFrame")
        controls.pack(fill="x", pady=10)

        self.start_btn = ttk.Button(controls, text="Start", style="Accent.TButton", command=self._start_process)
        self.start_btn.pack(side="left")

        self.stop_btn = ttk.Button(controls, text="Stop", command=self._stop_process)
        self.stop_btn.pack(side="left", padx=8)

        self.clear_btn = ttk.Button(controls, text="Clear Log", command=self._clear_log)
        self.clear_btn.pack(side="left", padx=8)

        self.quick_yes_btn = ttk.Button(controls, text="Send Y", command=lambda: self._send_quick("Y"))
        self.quick_yes_btn.pack(side="left", padx=(18, 6))

        self.quick_no_btn = ttk.Button(controls, text="Send N", command=lambda: self._send_quick("N"))
        self.quick_no_btn.pack(side="left")

        self.status_label = ttk.Label(controls, text="Status: Idle", style="Muted.TLabel")
        self.status_label.pack(side="right")

        output_card = ttk.LabelFrame(shell, text="Live Output", style="Card.TLabelframe", padding=8)
        output_card.pack(fill="both", expand=True)

        self.log_text = tk.Text(
            output_card,
            wrap="word",
            height=26,
            bg="#0E1523",
            fg="#DCE6FF",
            insertbackground="#DCE6FF",
            relief="flat",
            padx=10,
            pady=10,
            font=("Consolas", 10),
        )
        self.log_text.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(output_card, orient="vertical", command=self.log_text.yview)
        scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scroll.set)

        input_bar = ttk.Frame(shell, style="App.TFrame")
        input_bar.pack(fill="x", pady=(8, 0))

        ttk.Label(input_bar, text="Console Input", style="App.TLabel").pack(side="left")
        self.input_entry = ttk.Entry(input_bar, textvariable=self.input_var)
        self.input_entry.pack(side="left", fill="x", expand=True, padx=8)
        self.input_entry.bind("<Return>", self._on_enter_input)

        self.send_btn = ttk.Button(input_bar, text="Send", command=self._send_input)
        self.send_btn.pack(side="left")

    def _build_command(self) -> list[str]:
        if not TRACKER_SCRIPT.exists():
            raise FileNotFoundError(f"Script not found: {TRACKER_SCRIPT}")

        cmd = [sys.executable, "-u", str(TRACKER_SCRIPT)]

        interval_val = self.interval_var.get().strip()
        if interval_val:
            int(interval_val)
            cmd.extend(["--interval", interval_val])

        symbol_text = self.symbols_var.get().strip()
        if symbol_text:
            symbols = [part.strip().upper() for part in symbol_text.split() if part.strip()]
            if symbols:
                cmd.append("--symbols")
                cmd.extend(symbols)

        buy_protection = self.buy_rsi_protection_var.get().strip()
        if buy_protection:
            float(buy_protection)
            cmd.extend(["--buy-rsi-protection", buy_protection])

        min_profit = self.min_profit_pct_var.get().strip()
        if min_profit:
            float(min_profit)
            cmd.extend(["--min-profit-pct", min_profit])

        if self.hybrid_var.get():
            cmd.append("--hybrid")
        if self.telegram_var.get():
            cmd.append("--telegram")
        if self.confirm_order_var.get():
            cmd.append("--confirmOrder")
        if self.dry_run_var.get():
            cmd.append("--dry-run")
        if self.results_var.get():
            cmd.append("--results")

        if self.upstox_live_var.get():
            cmd.append("--upstox-live")
        else:
            cmd.append("--no-upstox-live")

        if not self.sync_daily_data_var.get():
            cmd.append("--no-sync-daily-data")

        return cmd

    def _start_process(self) -> None:
        if self.process is not None:
            messagebox.showinfo("Already running", "live_rsi_tracking.py is already running.")
            return

        try:
            cmd = self._build_command()
        except ValueError as exc:
            messagebox.showerror("Invalid input", f"Please check numeric inputs.\n\n{exc}")
            return
        except FileNotFoundError as exc:
            messagebox.showerror("Missing file", str(exc))
            return

        self._append_log("\nStarting command:\n" + " ".join(cmd) + "\n\n")

        creation_flags = 0
        if os.name == "nt":
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

        child_env = dict(os.environ)
        child_env["PYTHONUNBUFFERED"] = "1"

        self.process = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            text=True,
            bufsize=0,
            env=child_env,
            creationflags=creation_flags,
        )

        self._set_running_state(True)
        self.status_label.configure(text=f"Status: Running (PID {self.process.pid})")

        thread = threading.Thread(target=self._stream_output_worker, daemon=True)
        thread.start()

    def _stream_output_worker(self) -> None:
        proc = self.process
        if proc is None or proc.stdout is None:
            return

        while True:
            chunk = proc.stdout.read(1)
            if chunk == "":
                break
            self.log_queue.put(chunk)

        proc.wait()
        self.log_queue.put(f"\n\nProcess exited with code {proc.returncode}\n")
        self.log_queue.put("__PROCESS_DONE__")

    def _stop_process(self) -> None:
        if self.process is None:
            return

        self._append_log("\nStopping process...\n")
        try:
            self.process.terminate()
        except Exception as exc:
            self._append_log(f"Failed to stop process: {exc}\n")

    def _send_quick(self, value: str) -> None:
        self.input_var.set(value)
        self._send_input()

    def _on_enter_input(self, _event: tk.Event) -> None:
        self._send_input()

    def _send_input(self) -> None:
        if self.process is None or self.process.stdin is None:
            messagebox.showinfo("Not running", "Start the process before sending input.")
            return

        text = self.input_var.get().strip()
        if not text:
            return

        try:
            self.process.stdin.write(text + "\n")
            self.process.stdin.flush()
            self._append_log(f"\n> {text}\n")
            self.input_var.set("")
        except Exception as exc:
            self._append_log(f"\nFailed to send input: {exc}\n")

    def _drain_log_queue(self) -> None:
        try:
            while True:
                item = self.log_queue.get_nowait()
                if item == "__PROCESS_DONE__":
                    self.process = None
                    self._set_running_state(False)
                    self.status_label.configure(text="Status: Idle")
                    continue
                self._append_log(item)
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._drain_log_queue)

    def _set_running_state(self, running: bool) -> None:
        self.start_btn.configure(state=("disabled" if running else "normal"))
        self.stop_btn.configure(state=("normal" if running else "disabled"))
        self.send_btn.configure(state=("normal" if running else "disabled"))
        self.input_entry.configure(state=("normal" if running else "disabled"))
        self.quick_yes_btn.configure(state=("normal" if running else "disabled"))
        self.quick_no_btn.configure(state=("normal" if running else "disabled"))

    def _append_log(self, text: str) -> None:
        self.log_text.insert("end", text)
        self.log_text.see("end")

    def _clear_log(self) -> None:
        self.log_text.delete("1.0", "end")

    def _on_close(self) -> None:
        if self.process is not None:
            if not messagebox.askyesno("Exit", "A process is still running. Stop and exit?"):
                return
            self._stop_process()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    LiveRsiTrackingGui(root)
    root.mainloop()


if __name__ == "__main__":
    main()


