using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ProvisionSample
{
    public class Groups
    {
        //public Commands commands { get; private set; }

        public Tuple<string, Commands> CurrentGroup { get; private set; }

        public Commands TopLevelCommands { get; private set; }

        private List<Tuple<string, Commands>> m_commandGroups { get; set; }

        public Groups()
        {
            m_commandGroups = new List<Tuple<string, Commands>>();
            TopLevelCommands = new Commands();
        }
        public void AddGroup(string name, Commands commands)
        {
            commands.RegisterCommand("Exit group", ExitGroup);
            m_commandGroups.Add(new Tuple<string, Commands>(name, commands));
            var groupNum = m_commandGroups.Count - 1;
            TopLevelCommands.RegisterCommand(name, () => SetGroup(groupNum));
        }

        public Func<Task> GetCommand(bool switchGroup, int index)
        {
            if (!switchGroup && CurrentGroup != null)
                return CurrentGroup.Item2.GetCommand(index);

            return TopLevelCommands.GetCommand(index);
        }

        private async Task ExitGroup()
        {
            CurrentGroup = null;
        }

        private async Task SetGroup(int group)
        {
            CurrentGroup = m_commandGroups[group];
        }
    }
    public class Commands
    {
        private readonly List<Tuple<string, Func<Task>>> m_commands = new List<Tuple<string, Func<Task>>>();

        public void RegisterCommand(string description, Func<Task> operation)
        {
            m_commands.Add(Tuple.Create(description, operation));
        }

        public Func<Task> GetCommand(int commandNumber)
        {
            if (commandNumber >= m_commands.Count)
            {
                return null;
            }
            return m_commands[commandNumber].Item2;
        }

        public string GetCommandDescription(int commandNumber)
        {
            if (commandNumber >= m_commands.Count)
            {
                throw new Exception("Unknown command " + commandNumber);
            }
            return m_commands[commandNumber].Item1;
        }

        public int Count { get { return m_commands.Count; } }
    }

    /// <summary>
    /// Utilities for getting user insertion. To be overritten for processing scripts
    /// </summary>
    public class UserInput
    {
        public virtual int? EnsureIntParam(int? param, string desc, bool onlyFillIfEmpty = false, bool forceReEnter = false)
        {
            bool available = param.HasValue;
            if (onlyFillIfEmpty && available)
            {
                return param;
            }

            if (available)
            {
                ConsoleHelper.WriteColoredValue(desc, param.Value.ToString(), ConsoleColor.Magenta, forceReEnter ? ". Re-Enter same, or new int value:" : ". Press enter to use, or give new int value:");
            }
            else
            {
                Console.Write(desc + " is required. Enter int value:");
            }

            var entered = Console.ReadLine();
            int val;
            if (!string.IsNullOrWhiteSpace(entered))
            {
                if (!Int32.TryParse(entered, out val))
                {
                    Console.WriteLine("illegal int value:[" + entered + "]");
                    return null;
                }
                param = val;
            }
            return null;
        }

        public virtual string EnsureParam(string param, string desc, bool onlyFillIfEmpty = false, bool forceReEnter = false, bool isPassword = false)
        {
            bool available = !string.IsNullOrWhiteSpace(param);
            if (onlyFillIfEmpty && available)
            {
                return param;
            }

            if (available)
            {
                ConsoleHelper.WriteColoredValue(desc, param, ConsoleColor.Magenta, forceReEnter ? ". Re-Enter same, or new value:" : ". Press enter to use, or give new value:");
            }
            else
            {
                Console.Write(desc + " is required. Enter value:");
            }

            var entered = isPassword ? ConsoleHelper.ReadPassword() : Console.ReadLine();

            if (!string.IsNullOrWhiteSpace(entered))
            {
                param = entered;
            }

            return param;
        }

        public virtual string EnterOptionalParam(string desc, string skipResultDescription)
        {
            ConsoleHelper.WriteColoredValue(desc + " (optional). Enter value (or press Enter to ", skipResultDescription, ConsoleColor.Magenta, "):");

            var entered = Console.ReadLine();
            Console.WriteLine();
            if (!string.IsNullOrWhiteSpace(entered))
            {
                return entered;
            }

            return null;
        }
        public virtual string ManageCachedParam(string param, string desc, bool forceReset = false)
        {
            if (forceReset)
            {
                return null;
            }

            if (!string.IsNullOrWhiteSpace(param))
            {
                ConsoleHelper.WriteColoredValue(desc, param, ConsoleColor.Magenta, ". Enter 'Y': to Reset, 'A': to assign, Q: to Quit, Any another key to skip:");
            }
            else
            {
                ConsoleHelper.WriteColoredValue(desc, param, ConsoleColor.Magenta, ". Enter 'A': to assign, Q: to Quit, Any another key to skip:");
            }
            var ch = Char.ToUpper(Console.ReadKey().KeyChar);
            Console.WriteLine();
            switch (ch)
            {
                case 'Y':
                    return null;
                case 'A':
                    param = EnsureParam(null, desc);
                    break;
                case 'Q':
                    throw new Exception(string.Format("Quit managing cache when on '{0}', value ={1}", desc, param));
                default:
                    break;
            }
            return param;
        }

        public virtual void GetUserCommandSelection(out bool switchGroup, out int? numericCommand)
        {
            numericCommand = null;
            while (true)
            {
                var command = Console.ReadLine();
                if (string.IsNullOrEmpty(command))
                {
                    Console.WriteLine("No input. Try again");
                    continue;
                }

                int val;
                if(command[0] == '#')
                {
                    switchGroup = true;
                    command = command.TrimStart('#');
                }
                else
                {
                    switchGroup = false;
                }

                if (int.TryParse(command, out val))
                {
                    numericCommand = val;
                    return;
                }
                
                Console.WriteLine("Illegal input. Try again");
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class ConsoleHelper
    {
        public static void PrintCommands(Groups group)
        {
            Console.WriteLine();
            WriteColoredValue("What do you want to do (select ", "numeric", ConsoleColor.Green, " value)?", showEquals:false, newLine:true);
            Commands commands;
            if (group.CurrentGroup != null)
            {
                WriteColoredValue("Current group", group.CurrentGroup.Item1, ConsoleColor.Magenta, newLine:true);
                WriteColoredValue("You can use ", "#1, #2 ... ", ConsoleColor.Green, "to quickly switch to another group", showEquals:false, newLine:true);
                commands = group.CurrentGroup.Item2;
            }
            else
            {
                Console.WriteLine("Select command group:");
                commands = group.TopLevelCommands;
            }
            Console.WriteLine("=================================================================");
            
            for (int i = 0; i < commands.Count; i++)
            {
                var numericSize = i < 9 ? 1 : ((i < 99) ? 2 : 3);
                var align = i < 9 ? " " : "";
                WriteColoredStringLine(string.Format("{0} {1} {2}", i + 1, align,commands.GetCommandDescription(i)), ConsoleColor.Green, numericSize);
            }
            Console.WriteLine();
        }

        public static void WriteColoredStringLine(string text, ConsoleColor color, int coloredChars)
        {
            Console.ForegroundColor = color;
            Console.Write(text.Substring(0, coloredChars));
            Console.ResetColor();
            Console.WriteLine(text.Substring(coloredChars));
        }

        public static void WriteColoredValue(string desc, string param, ConsoleColor color, string restOfLine = null, bool showEquals = true, bool newLine = false)
        {
            Console.Write(desc);
            if (showEquals)
            {
                Console.Write(" = ");
            }

            Console.ForegroundColor = color;
            Console.Write(param);
            Console.ResetColor();
            if (restOfLine != null)
                Console.Write(restOfLine);

            if(newLine)
            {
                Console.WriteLine();
            }
        }

        public static string ReadPassword()
        {
            ConsoleKeyInfo key;
            var password = string.Empty;

            do
            {
                key = Console.ReadKey(true);
                if (key.Key != ConsoleKey.Backspace && key.Key != ConsoleKey.Enter)
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                    {
                        password = password.Substring(0, (password.Length - 1));
                        Console.Write("\b \b");
                    }
                }
            }
            // Stops Receving Keys Once Enter is Pressed
            while (key.Key != ConsoleKey.Enter);

            Console.WriteLine();
            return password;
        }
    }
}
