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
from InquirerPy.base.control import Choice
from prompt_toolkit.completion import PathCompleter
from rich.console import Console
from rich.markdown import Markdown
from rich.progress import Progress

console = Console()

# Global variable to store the remembered file path
REMEMBERED_FILE_PATH = None

# ---------- 1. Minimal command registry ----------

COMMANDS = {
    "remember file": {
        "args": [
            {"name": "file", "kind": "path", "positional": True},
        ]
    },
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
            {"name": "--fast", "kind": "flag", "default": False},
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
    global REMEMBERED_FILE_PATH
    try:
        if multi:
            paths = []
            while True:
                # For multi-path selection, we don't use the remembered path
                path = inquirer.filepath(
                    message=f"{message} (Enter to finish after selecting at least one)",
                    only_files=False,  # Allow directories to be shown for navigation
                    validate=lambda result: True if result or paths else "Please select at least one file",
                    mandatory=False  # Allow empty input to handle ESC key
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
            # Show the remembered path in the message if it exists
            display_message = message
            if REMEMBERED_FILE_PATH and Path(REMEMBERED_FILE_PATH).is_file():
                display_message = f"{message} [dim](remembered: {REMEMBERED_FILE_PATH})[/]"
                # Offer to use the remembered path
                use_remembered = inquirer.confirm(
                    message=f"Use remembered file: {REMEMBERED_FILE_PATH}?",
                    default=True
                ).execute()
                if use_remembered:
                    return REMEMBERED_FILE_PATH
            
            while True:
                path = inquirer.filepath(
                    message=display_message,
                    only_files=False,  # Allow directories to be shown for navigation
                    mandatory=False  # Allow empty input to handle ESC key
                ).execute()
                
                # Check if the selected path is a file
                if path and Path(path).is_file():
                    return path
                elif path:
                    console.print(f"[yellow]Please select a file, not a directory[/]")
                elif path == "":
                    # User pressed ESC or entered empty string
                    raise KeyboardInterrupt()
    except KeyboardInterrupt:
        console.print("[yellow]Returning to main menu...[/]")
        raise


def ask_int(message, default):
    try:
        return inquirer.number(
            message=f"{message} (default: {default})",
            validate=NumberValidator(),
            default=default,
            mandatory=False  # Allow empty input to handle ESC key
        ).execute()
    except KeyboardInterrupt:
        console.print("[yellow]Returning to main menu...[/]")
        raise


def ask_flag(message, default=False):
    try:
        return inquirer.confirm(message=message, default=default).execute()
    except KeyboardInterrupt:
        console.print("[yellow]Returning to main menu...[/]")
        raise


def ask_text(message):
    try:
        return inquirer.text(
            message=message,
            mandatory=False  # Allow empty input to handle ESC key
        ).execute()
    except KeyboardInterrupt:
        console.print("[yellow]Returning to main menu...[/]")
        raise


def ask_output_path(message):
    try:
        while True:
            path = inquirer.filepath(
                message=message,
                only_files=False,  # Allow directories to be shown for navigation
                validate=lambda result: True if result else "Please select a path",
                mandatory=False  # Allow empty input to handle ESC key
            ).execute()
            
            # Check if the selected path is a directory
            if path and Path(path).is_dir():
                console.print(f"[yellow]Please select a file, not a directory[/]")
            elif path == "":
                # User pressed ESC or entered empty string
                raise KeyboardInterrupt()
            else:
                return path
    except KeyboardInterrupt:
        console.print("[yellow]Returning to main menu...[/]")
        raise


def handle_sql_suggestion(command_str, output_lines):
    """Extract and handle SQL suggestions from AI assistance.
    
    Args:
        command_str: The original command string
        output_lines: List of output lines from command execution
        
    Returns:
        bool: True if a suggestion was found and executed, False otherwise
    """
    output_text = '\n'.join(output_lines)
    
    # Look for indicators of AI suggestion - simpler approach
    if 'Suggested fix:' not in output_text and '💡' not in output_text:
        return False
        
    # Extract the suggested SQL query
    sql_code = None
    in_code_block = False
    code_lines = []
    
    # Process each line to extract SQL code
    for line in output_lines:
        # Check for code block markers
        if '```sql' in line:
            in_code_block = True
            continue
        elif '```' in line and in_code_block:
            in_code_block = False
            sql_code = '\n'.join(code_lines)
            break
        elif in_code_block:
            code_lines.append(line)
    
    # If no code block found, try to extract SQL statements directly
    if not sql_code:
        for i, line in enumerate(output_lines):
            if any(keyword in line.upper() for keyword in ['SELECT ', 'CREATE ', 'INSERT ', 'UPDATE ', 'DELETE ', 'WITH ']):
                # Found a line with SQL keywords
                sql_start = i
                sql_lines = [line]
                # Collect SQL lines until we hit a likely end
                for j in range(sql_start + 1, len(output_lines)):
                    if not output_lines[j].strip() or ';' in output_lines[j]:
                        if ';' in output_lines[j]:
                            sql_lines.append(output_lines[j])
                        break
                    sql_lines.append(output_lines[j])
                sql_code = '\n'.join(sql_lines)
                break
    
    if sql_code:
        # Clean up the SQL code
        sql_code = sql_code.strip()
        # Remove trailing semicolon if present
        if sql_code.endswith(';'):
            sql_code = sql_code[:-1]
        
        console.print("\n[bold cyan]AI suggested a SQL query fix.[/]")
        if ask_flag("Would you like to run the suggested SQL query?", True):
            # Extract the original command parts and replace the SQL query
            parts = shlex.split(command_str)
            # Find the SQL query position (last argument)
            parts[-1] = sql_code
            new_command = shlex.join(parts)
            console.print(f"[dim]Will run:[/] {new_command}")
            run_cli(new_command)
            return True
    
    return False


def run_cli(command_str):
    if command_str is None:
        return
        
    console.rule(f"[bold green]Executing[/] {command_str}")

    # stream output live while showing Rich progress spinner
    output_lines = []
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
            line_text = line.rstrip()
            output_lines.append(line_text)
            console.print(line_text)
        proc.wait()
        prog.update(task, completed=100)

    # Check if this is a query command with AI suggestion
    if 'query' in command_str:
        handled = handle_sql_suggestion(command_str, output_lines)

    if proc.returncode != 0:
        console.print(f"[bold red]omo-cli exited with code {proc.returncode}[/]")
    else:
        console.print("[bold green]\u2713 Done[/]")


def build_command(cmd_name):
    """Build a command string based on user input for the specified command."""
    global REMEMBERED_FILE_PATH
    spec = COMMANDS[cmd_name]
    parts = ["omo-cli"]
    
    # Handle the special case for the "remember file" command
    if cmd_name == "remember file":
        try:
            file_path = ask_path("Select file to remember")
            if file_path:
                REMEMBERED_FILE_PATH = file_path
                console.print(f"[bold green]Remembered file path:[/] {REMEMBERED_FILE_PATH}")
            return None  # No command to execute for remember
        except KeyboardInterrupt:
            return None
    
    # For regular commands
    parts.append(cmd_name)

    try:
        # Special handling for stats command with --fast option
        if cmd_name == "stats":
            # First ask if the user wants to use the fast option
            use_fast = ask_flag("Use fast mode (DuckDB-based statistics)?")
            if use_fast:
                parts.append("--fast")
                # When using fast mode, only process the file and format options
                for arg in spec["args"]:
                    kind = arg["kind"]
                    name = arg["name"]
                    is_positional = arg.get("positional", False)
                    
                    # Only allow file and format options with --fast
                    if name == "file" or name == "--format":
                        if kind == "path":
                            value = ask_path(f"Path for {name.lstrip('-')}")
                            if is_positional:
                                parts.append(str(value))
                            else:
                                parts.extend([name, str(value)])
                        elif kind == "text":
                            value = ask_text(f"{name.lstrip('-')} (leave blank to skip)")
                            if value:
                                if is_positional:
                                    parts.append(value)
                                else:
                                    parts.extend([name, value])
                return shlex.join(parts)  # Return early with only these options
        
        # Normal processing for all other commands or stats without --fast
        for arg in spec["args"]:
            kind = arg["kind"]
            name = arg["name"]
            is_positional = arg.get("positional", False)
            
            # Skip the --fast option for stats if we're in the normal path
            if cmd_name == "stats" and name == "--fast":
                continue

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
    except KeyboardInterrupt:
        return None


def app():
    console.print(Markdown("# OmniMorph Wizard 🤖"))
    global REMEMBERED_FILE_PATH

    while True:
        try:
            # Show the remembered file in the main menu if it exists
            if REMEMBERED_FILE_PATH and Path(REMEMBERED_FILE_PATH).is_file():
                console.print(f"[dim]Remembered file:[/] {REMEMBERED_FILE_PATH}")
            
            # Create choices list with remember/forget at the top
            command_choices = []
            if REMEMBERED_FILE_PATH:
                command_choices.append("forget file")
            else:
                command_choices.append("remember file")
                
            # Add all other commands
            command_choices.extend(list(COMMANDS))
            # Add quit option at the end
            command_choices.append("QUIT")
            
            cmd_name = inquirer.select(                    # list prompt
                message="Choose a command",
                choices=command_choices,
                long_instruction="Arrow keys to move ‣ Enter to select ‣ CTRL-C to cancel",
            ).execute()

            if cmd_name == "QUIT":
                console.print("Good-bye!")
                sys.exit(0)
            elif cmd_name == "forget file":
                if REMEMBERED_FILE_PATH:
                    REMEMBERED_FILE_PATH = None
                    console.print("[bold yellow]Cleared remembered file path[/]")
                else:
                    console.print("[dim]No file path was remembered[/]")
                continue

            command_str = build_command(cmd_name)
            if command_str is None:
                continue
                
            console.print(f"[dim]Will run:[/] {command_str}")
            try:
                if ask_flag("Proceed?", True):
                    run_cli(command_str)
            except KeyboardInterrupt:
                console.print("[yellow]Cancelled execution[/]")
                continue
        except KeyboardInterrupt:
            # Handle ESC at the top level menu
            if ask_flag("Do you want to quit?", False):
                console.print("Good-bye!")
                sys.exit(0)


if __name__ == "__main__":
    app()