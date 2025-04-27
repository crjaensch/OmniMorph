#!/usr/bin/env python3
"""
omo-wizard - interactive front-end for the OmniMorph CLI (omo-cli)
"""

import shlex
import subprocess
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.validator import NumberValidator
from prompt_toolkit.completion import PathCompleter
from rich.console import Console
from rich.markdown import Markdown
from rich.progress import Progress

console = Console()

# ---------- 1. Minimal command registry ----------

COMMANDS = {
    "head": {
        "args": [
            {"name": "--number", "kind": "int", "default": 10},
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
    "tail": {
        "args": [
            {"name": "--number", "kind": "int", "default": 10},
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
    "meta": {
        "args": [
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
    "schema": {
        "args": [
            {"name": "--markdown", "kind": "flag", "default": False},
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
    "stats": {
        "args": [
            {"name": "--markdown", "kind": "flag", "default": False},
            {"name": "--columns", "kind": "text", "optional": True},
            {"name": "--format", "kind": "text", "optional": True},
            {"name": "--sample-size", "kind": "int", "default": 2048},
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
    "query": {
        "args": [
            {"name": "--format", "kind": "text", "optional": True},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "sql_query", "kind": "sql", "positional": True},
        ]
    },
    "random-sample": {
        "args": [
            {"name": "--n", "kind": "int", "optional": True},
            {"name": "--fraction", "kind": "float", "optional": True},
            {"name": "--seed", "kind": "int", "optional": True},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
    "to-avro": {
        "args": [
            {"name": "--compression", "kind": "text", "default": "uncompressed"},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
    "to-csv": {
        "args": [
            {"name": "--has-header", "kind": "flag", "default": True},
            {"name": "--delimiter", "kind": "text", "default": ","},
            {"name": "--quote", "kind": "text", "default": "\""},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
    "to-json": {
        "args": [
            {"name": "--pretty", "kind": "flag", "default": False},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
    "to-parquet": {
        "args": [
            {"name": "--compression", "kind": "text", "default": "uncompressed"},
            {"name": "file", "kind": "path", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
    "merge": {
        "args": [
            {"name": "--allow-cast", "kind": "flag", "default": True},
            {"name": "--progress", "kind": "flag", "default": False},
            {"name": "files", "kind": "paths", "positional": True},
            {"name": "output", "kind": "output_path", "positional": True},
        ]
    },
}

# ---------- 2. Helpers ----------

def ask_path(message="Select file", multi=False):
    if multi:
        paths = []
        while True:
            path = inquirer.filepath(
                message=f"{message} (Enter to finish after selecting at least one)",
                only_files=False,  # Allow directories to be shown for navigation
                validate=lambda result: True if result or paths else "Please select at least one file"
            ).execute()
            
            if not path and paths:  # Empty input after at least one file
                break
            elif path:
                # Check if the selected path is a file
                if Path(path).is_file():
                    paths.append(path)
                    console.print(f"[dim]Added:[/] {path}")
                else:
                    console.print(f"[yellow]Please select a file, not a directory[/]")
        
        return paths
    else:
        while True:
            path = inquirer.filepath(
                message=message,
                only_files=False,  # Allow directories to be shown for navigation
            ).execute()
            
            # Check if the selected path is a file
            if path and Path(path).is_file():
                return path
            elif path:
                console.print(f"[yellow]Please select a file, not a directory[/]")


def ask_int(message, default):
    return inquirer.number(
        message=f"{message} (default: {default})",
        validate=NumberValidator(),
        default=default,
    ).execute()


def ask_flag(message, default=False):
    return inquirer.confirm(message=message, default=default).execute()


def ask_text(message):
    return inquirer.text(message=message).execute()


def ask_output_path(message):
    while True:
        path = inquirer.filepath(
            message=message,
            only_files=False,  # Allow directories to be shown for navigation
            validate=lambda result: True if result else "Please select a path"
        ).execute()
        
        # Check if the selected path is a directory
        if path and Path(path).is_dir():
            console.print(f"[yellow]Please select a file, not a directory[/]")
        else:
            return path


# ---------- 3. Main loop ----------

def build_command(cmd_name):
    spec = COMMANDS[cmd_name]
    parts = ["omo-cli", cmd_name]

    for arg in spec["args"]:
        kind = arg["kind"]
        name = arg["name"]
        is_positional = arg.get("positional", False)

        if kind == "path":
            value = ask_path(f"Path for {name.lstrip('-')}")
            if is_positional:
                parts.append(str(value))
            else:
                parts.extend([name, str(value)])

        elif kind == "paths":
            value = ask_path(f"Paths for {name.lstrip('-')}", multi=True)
            if is_positional:
                parts.extend([str(v) for v in value])
            else:
                parts.append(name)
                parts.extend([str(v) for v in value])

        elif kind == "int":
            value = ask_int(name.lstrip("-"), arg.get("default", 0))
            if is_positional:
                parts.append(str(value))
            else:
                parts.extend([name, str(value)])

        elif kind == "float":
            value = ask_text(f"{name.lstrip('-')} (default: {arg.get('default', 0)})")
            if value:
                if is_positional:
                    parts.append(value)
                else:
                    parts.extend([name, value])

        elif kind == "flag":
            if ask_flag(f"Enable {name.lstrip('-')}", default=arg.get("default", False)):
                parts.append(name)

        elif kind == "text":
            value = ask_text(f"{name.lstrip('-')} (leave blank to skip)")
            if value:
                if is_positional:
                    parts.append(value)
                else:
                    parts.extend([name, value])

        elif kind == "sql":
            value = ask_text(f"{name.lstrip('-')} (SQL query)")
            if value:
                parts.append(value)

        elif kind == "output_path":
            value = ask_output_path(f"Output path for {name.lstrip('-')}")
            if is_positional:
                parts.append(str(value))
            else:
                parts.extend([name, str(value)])

    return shlex.join(parts)


def run_cli(command_str):
    console.rule(f"[bold green]Executing[/] {command_str}")

    # stream output live while showing Rich progress spinner
    with Progress(transient=True) as prog:
        task = prog.add_task("omo-cli", start=False)
        proc = subprocess.Popen(
            command_str,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        prog.start_task(task)
        for line in proc.stdout:
            console.print(line.rstrip())
        proc.wait()
        prog.update(task, completed=100)

    if proc.returncode != 0:
        console.print(f"[bold red]omo-cli exited with code {proc.returncode}[/]")
    else:
        console.print("[bold green]✓ Done[/]")


def app():
    console.print(Markdown("# OmniMorph Wizard 🤖"))

    while True:
        cmd_name = inquirer.select(                    # list prompt
            message="Choose a command",
            choices=list(COMMANDS) + ["QUIT"],
            long_instruction="Arrow keys to move ‣ Enter to select",
        ).execute()

        if cmd_name == "QUIT":
            console.print("Good-bye!")
            sys.exit(0)

        command_str = build_command(cmd_name)
        console.print(f"[dim]Will run:[/] {command_str}")
        if ask_flag("Proceed?", True):
            run_cli(command_str)


if __name__ == "__main__":
    app()