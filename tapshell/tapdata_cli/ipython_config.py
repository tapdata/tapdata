# Configuration file for ipython.

#------------------------------------------------------------------------------
# InteractiveShellApp(Configurable) configuration
#------------------------------------------------------------------------------
## A Mixin for applications that start InteractiveShell instances.
#  
#      Provides configurables for loading extensions and executing files
#      as part of configuring a Shell environment.
#  
#      The following methods should be called by the :meth:`initialize` method
#      of the subclass:
#  
#        - :meth:`init_path`
#        - :meth:`init_shell` (to be implemented by the subclass)
#        - :meth:`init_gui_pylab`
#        - :meth:`init_extensions`
#        - :meth:`init_code`

## Execute the given command string.
#  Default: ''
# c.InteractiveShellApp.code_to_run = ''

## Run the file referenced by the PYTHONSTARTUP environment
#          variable at IPython startup.
#  Default: True
# c.InteractiveShellApp.exec_PYTHONSTARTUP = True

## List of files to run at IPython startup.
#  Default: []
# c.InteractiveShellApp.exec_files = []

## lines of code to run at IPython startup.
#  Default: []
# c.InteractiveShellApp.exec_lines = []

## A list of dotted module names of IPython extensions to load.
#  Default: []
# c.InteractiveShellApp.extensions = []

## Dotted module name(s) of one or more IPython extensions to load.
#  
#  For specifying extra extensions to load on the command-line.
#  
#  .. versionadded:: 7.10
#  Default: []
# c.InteractiveShellApp.extra_extensions = []

## A file to be run
#  Default: ''
# c.InteractiveShellApp.file_to_run = ''

## Enable GUI event loop integration with any of ('asyncio', 'glut', 'gtk',
#  'gtk2', 'gtk3', 'gtk4', 'osx', 'pyglet', 'qt', 'qt4', 'qt5', 'qt6', 'tk',
#  'wx', 'gtk2', 'qt4').
#  Choices: any of ['asyncio', 'glut', 'gtk', 'gtk2', 'gtk3', 'gtk4', 'osx', 'pyglet', 'qt', 'qt4', 'qt5', 'qt6', 'tk', 'wx', 'gtk2', 'qt4'] (case-insensitive) or None
#  Default: None
# c.InteractiveShellApp.gui = None

## Should variables loaded at startup (by startup files, exec_lines, etc.)
#          be hidden from tools like %who?
#  Default: True
# c.InteractiveShellApp.hide_initial_ns = True

## If True, IPython will not add the current working directory to sys.path.
#          When False, the current working directory is added to sys.path, allowing imports
#          of modules defined in the current directory.
#  Default: False
# c.InteractiveShellApp.ignore_cwd = False

## Configure matplotlib for interactive use with
#          the default matplotlib backend.
#  Choices: any of ['auto', 'agg', 'gtk', 'gtk3', 'gtk4', 'inline', 'ipympl', 'nbagg', 'notebook', 'osx', 'pdf', 'ps', 'qt', 'qt4', 'qt5', 'qt6', 'svg', 'tk', 'widget', 'wx'] (case-insensitive) or None
#  Default: None
# c.InteractiveShellApp.matplotlib = None

## Run the module as a script.
#  Default: ''
# c.InteractiveShellApp.module_to_run = ''

## Pre-load matplotlib and numpy for interactive use,
#          selecting a particular matplotlib backend and loop integration.
#  Choices: any of ['auto', 'agg', 'gtk', 'gtk3', 'gtk4', 'inline', 'ipympl', 'nbagg', 'notebook', 'osx', 'pdf', 'ps', 'qt', 'qt4', 'qt5', 'qt6', 'svg', 'tk', 'widget', 'wx'] (case-insensitive) or None
#  Default: None
# c.InteractiveShellApp.pylab = None

## If true, IPython will populate the user namespace with numpy, pylab, etc.
#          and an ``import *`` is done from numpy and pylab, when using pylab mode.
#  
#          When False, pylab mode should not import any names into the user
#  namespace.
#  Default: True
# c.InteractiveShellApp.pylab_import_all = True

## Reraise exceptions encountered loading IPython extensions?
#  Default: False
# c.InteractiveShellApp.reraise_ipython_extension_failures = False

#------------------------------------------------------------------------------
# Application(SingletonConfigurable) configuration
#------------------------------------------------------------------------------
## This is an application.

## The date format used by logging formatters for %(asctime)s
#  Default: '%Y-%m-%d %H:%M:%S'
# c.Application.log_datefmt = '%Y-%m-%d %H:%M:%S'

## The Logging format template
#  Default: '[%(name)s]%(highlevel)s %(message)s'
# c.Application.log_format = '[%(name)s]%(highlevel)s %(message)s'

## Set the log level by value or name.
#  Choices: any of [0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']
#  Default: 30
# c.Application.log_level = 30

## Instead of starting the Application, dump configuration to stdout
#  Default: False
# c.Application.show_config = False

## Instead of starting the Application, dump configuration to stdout (as JSON)
#  Default: False
# c.Application.show_config_json = False

#------------------------------------------------------------------------------
# BaseIPythonApplication(Application) configuration
#------------------------------------------------------------------------------
## IPython: an enhanced interactive Python shell.

#  Default: False
# c.BaseIPythonApplication.add_ipython_dir_to_sys_path = False

## Whether to create profile dir if it doesn't exist
#  Default: False
# c.BaseIPythonApplication.auto_create = False

## Whether to install the default config files into the profile dir.
#          If a new profile is being created, and IPython contains config files for that
#          profile, then they will be staged into the new directory.  Otherwise,
#          default config files will be automatically generated.
#  Default: False
# c.BaseIPythonApplication.copy_config_files = False

## Path to an extra config file to load.
#  
#      If specified, load this config file in addition to any other IPython
#  config.
#  Default: ''
# c.BaseIPythonApplication.extra_config_file = ''

## The name of the IPython directory. This directory is used for logging
#  configuration (through profiles), history storage, etc. The default is usually
#  $HOME/.ipython. This option can also be specified through the environment
#  variable IPYTHONDIR.
#  Default: ''
# c.BaseIPythonApplication.ipython_dir = ''

## The date format used by logging formatters for %(asctime)s
#  See also: Application.log_datefmt
# c.BaseIPythonApplication.log_datefmt = '%Y-%m-%d %H:%M:%S'

## The Logging format template
#  See also: Application.log_format
# c.BaseIPythonApplication.log_format = '[%(name)s]%(highlevel)s %(message)s'

## Set the log level by value or name.
#  See also: Application.log_level
# c.BaseIPythonApplication.log_level = 30

## Whether to overwrite existing config files when copying
#  Default: False
# c.BaseIPythonApplication.overwrite = False

## The IPython profile to use.
#  Default: 'default'
# c.BaseIPythonApplication.profile = 'default'

## Instead of starting the Application, dump configuration to stdout
#  See also: Application.show_config
# c.BaseIPythonApplication.show_config = False

## Instead of starting the Application, dump configuration to stdout (as JSON)
#  See also: Application.show_config_json
# c.BaseIPythonApplication.show_config_json = False

## Create a massive crash report when IPython encounters what may be an
#          internal error.  The default is to append a short message to the
#          usual traceback
#  Default: False
# c.BaseIPythonApplication.verbose_crash = False

#------------------------------------------------------------------------------
# TerminalIPythonApp(BaseIPythonApplication, InteractiveShellApp) configuration
#------------------------------------------------------------------------------
#  See also: BaseIPythonApplication.add_ipython_dir_to_sys_path
# c.TerminalIPythonApp.add_ipython_dir_to_sys_path = False

## Execute the given command string.
#  See also: InteractiveShellApp.code_to_run
# c.TerminalIPythonApp.code_to_run = ''

## Whether to install the default config files into the profile dir.
#  See also: BaseIPythonApplication.copy_config_files
# c.TerminalIPythonApp.copy_config_files = False

## Whether to display a banner upon starting IPython.
#  Default: True
# c.TerminalIPythonApp.display_banner = True

## Run the file referenced by the PYTHONSTARTUP environment
#  See also: InteractiveShellApp.exec_PYTHONSTARTUP
# c.TerminalIPythonApp.exec_PYTHONSTARTUP = True

## List of files to run at IPython startup.
#  See also: InteractiveShellApp.exec_files
# c.TerminalIPythonApp.exec_files = []

## lines of code to run at IPython startup.
#  See also: InteractiveShellApp.exec_lines
# c.TerminalIPythonApp.exec_lines = []

## A list of dotted module names of IPython extensions to load.
#  See also: InteractiveShellApp.extensions
# c.TerminalIPythonApp.extensions = []

## Path to an extra config file to load.
#  See also: BaseIPythonApplication.extra_config_file
# c.TerminalIPythonApp.extra_config_file = ''

## 
#  See also: InteractiveShellApp.extra_extensions
# c.TerminalIPythonApp.extra_extensions = []

## A file to be run
#  See also: InteractiveShellApp.file_to_run
# c.TerminalIPythonApp.file_to_run = ''

## If a command or file is given via the command-line,
#          e.g. 'ipython foo.py', start an interactive shell after executing the
#          file or command.
#  Default: False
# c.TerminalIPythonApp.force_interact = False

## Enable GUI event loop integration with any of ('asyncio', 'glut', 'gtk',
#  'gtk2', 'gtk3', 'gtk4', 'osx', 'pyglet', 'qt', 'qt4', 'qt5', 'qt6', 'tk',
#  'wx', 'gtk2', 'qt4').
#  See also: InteractiveShellApp.gui
# c.TerminalIPythonApp.gui = None

## Should variables loaded at startup (by startup files, exec_lines, etc.)
#  See also: InteractiveShellApp.hide_initial_ns
# c.TerminalIPythonApp.hide_initial_ns = True

## If True, IPython will not add the current working directory to sys.path.
#  See also: InteractiveShellApp.ignore_cwd
# c.TerminalIPythonApp.ignore_cwd = False

## Class to use to instantiate the TerminalInteractiveShell object. Useful for
#  custom Frontends
#  Default: 'IPython.terminal.interactiveshell.TerminalInteractiveShell'
# c.TerminalIPythonApp.interactive_shell_class = 'IPython.terminal.interactiveshell.TerminalInteractiveShell'

## 
#  See also: BaseIPythonApplication.ipython_dir
# c.TerminalIPythonApp.ipython_dir = ''

## The date format used by logging formatters for %(asctime)s
#  See also: Application.log_datefmt
# c.TerminalIPythonApp.log_datefmt = '%Y-%m-%d %H:%M:%S'

## The Logging format template
#  See also: Application.log_format
# c.TerminalIPythonApp.log_format = '[%(name)s]%(highlevel)s %(message)s'

## Set the log level by value or name.
#  See also: Application.log_level
# c.TerminalIPythonApp.log_level = 30

## Configure matplotlib for interactive use with
#  See also: InteractiveShellApp.matplotlib
# c.TerminalIPythonApp.matplotlib = None

## Run the module as a script.
#  See also: InteractiveShellApp.module_to_run
# c.TerminalIPythonApp.module_to_run = ''

## Whether to overwrite existing config files when copying
#  See also: BaseIPythonApplication.overwrite
# c.TerminalIPythonApp.overwrite = False

## The IPython profile to use.
#  See also: BaseIPythonApplication.profile
# c.TerminalIPythonApp.profile = 'default'

## Pre-load matplotlib and numpy for interactive use,
#  See also: InteractiveShellApp.pylab
# c.TerminalIPythonApp.pylab = None

## If true, IPython will populate the user namespace with numpy, pylab, etc.
#  See also: InteractiveShellApp.pylab_import_all
# c.TerminalIPythonApp.pylab_import_all = True

## Start IPython quickly by skipping the loading of config files.
#  Default: False
# c.TerminalIPythonApp.quick = False

## Reraise exceptions encountered loading IPython extensions?
#  See also: InteractiveShellApp.reraise_ipython_extension_failures
# c.TerminalIPythonApp.reraise_ipython_extension_failures = False

## Instead of starting the Application, dump configuration to stdout
#  See also: Application.show_config
# c.TerminalIPythonApp.show_config = False

## Instead of starting the Application, dump configuration to stdout (as JSON)
#  See also: Application.show_config_json
# c.TerminalIPythonApp.show_config_json = False

## Create a massive crash report when IPython encounters what may be an
#  See also: BaseIPythonApplication.verbose_crash
# c.TerminalIPythonApp.verbose_crash = False

#------------------------------------------------------------------------------
# InteractiveShell(SingletonConfigurable) configuration
#------------------------------------------------------------------------------
## An enhanced, interactive shell for Python.

## 'all', 'last', 'last_expr' or 'none', 'last_expr_or_assign' specifying which
#  nodes should be run interactively (displaying output from expressions).
#  Choices: any of ['all', 'last', 'last_expr', 'none', 'last_expr_or_assign']
#  Default: 'last_expr'
# c.InteractiveShell.ast_node_interactivity = 'last_expr'

## A list of ast.NodeTransformer subclass instances, which will be applied to
#  user input before code is run.
#  Default: []
# c.InteractiveShell.ast_transformers = []

## Automatically run await statement in the top level repl.
#  Default: True
# c.InteractiveShell.autoawait = True

## Make IPython automatically call any callable object even if you didn't type
#  explicit parentheses. For example, 'str 43' becomes 'str(43)' automatically.
#  The value can be '0' to disable the feature, '1' for 'smart' autocall, where
#  it is not applied if there are no more arguments on the line, and '2' for
#  'full' autocall, where all callable objects are automatically called (even if
#  no arguments are present).
#  Choices: any of [0, 1, 2]
#  Default: 0
# c.InteractiveShell.autocall = 0

## Autoindent IPython code entered interactively.
#  Default: True
# c.InteractiveShell.autoindent = True

## Enable magic commands to be called without the leading %.
#  Default: True
# c.InteractiveShell.automagic = True

## The part of the banner to be printed before the profile
#  Default: "Python 3.9.10 (main, Jan 15 2022, 11:48:04) \nType 'copyright', 'credits' or 'license' for more information\nIPython 8.2.0 -- An enhanced Interactive Python. Type '?' for help.\n"
# c.InteractiveShell.banner1 = "Python 3.9.10 (main, Jan 15 2022, 11:48:04) \nType 'copyright', 'credits' or 'license' for more information\nIPython 8.2.0 -- An enhanced Interactive Python. Type '?' for help.\n"

## The part of the banner to be printed after the profile
#  Default: ''
# c.InteractiveShell.banner2 = ''

## Set the size of the output cache.  The default is 1000, you can change it
#  permanently in your config file.  Setting it to 0 completely disables the
#  caching system, and the minimum value accepted is 3 (if you provide a value
#  less than 3, it is reset to 0 and a warning is issued).  This limit is defined
#  because otherwise you'll spend more time re-flushing a too small cache than
#  working
#  Default: 1000
# c.InteractiveShell.cache_size = 1000

## Use colors for displaying information about objects. Because this information
#  is passed through a pager (like 'less'), and some pagers get confused with
#  color codes, this capability can be turned off.
#  Default: True
# c.InteractiveShell.color_info = True

## Set the color scheme (NoColor, Neutral, Linux, or LightBG).
#  Choices: any of ['Neutral', 'NoColor', 'LightBG', 'Linux'] (case-insensitive)
#  Default: 'Neutral'
# c.InteractiveShell.colors = 'Neutral'

#  Default: False
# c.InteractiveShell.debug = False

## Don't call post-execute functions that have failed in the past.
#  Default: False
# c.InteractiveShell.disable_failing_post_execute = False

## If True, anything that would be passed to the pager
#          will be displayed as regular output instead.
#  Default: False
# c.InteractiveShell.display_page = False

## (Provisional API) enables html representation in mime bundles sent to pagers.
#  Default: False
# c.InteractiveShell.enable_html_pager = False

## Total length of command history
#  Default: 10000
# c.InteractiveShell.history_length = 10000

## The number of saved history entries to be loaded into the history buffer at
#  startup.
#  Default: 1000
# c.InteractiveShell.history_load_length = 1000

#  Default: ''
# c.InteractiveShell.ipython_dir = ''

## Start logging to the given file in append mode. Use `logfile` to specify a log
#  file to **overwrite** logs to.
#  Default: ''
# c.InteractiveShell.logappend = ''

## The name of the logfile to use.
#  Default: ''
# c.InteractiveShell.logfile = ''

## Start logging to the default log file in overwrite mode. Use `logappend` to
#  specify a log file to **append** logs to.
#  Default: False
# c.InteractiveShell.logstart = False

## Select the loop runner that will be used to execute top-level asynchronous
#  code
#  Default: 'IPython.core.interactiveshell._asyncio_runner'
# c.InteractiveShell.loop_runner = 'IPython.core.interactiveshell._asyncio_runner'

#  Choices: any of [0, 1, 2]
#  Default: 0
# c.InteractiveShell.object_info_string_level = 0

## Automatically call the pdb debugger after every exception.
#  Default: False
# c.InteractiveShell.pdb = False

#  Default: False
# c.InteractiveShell.quiet = False

#  Default: '\n'
# c.InteractiveShell.separate_in = '\n'

#  Default: ''
# c.InteractiveShell.separate_out = ''

#  Default: ''
# c.InteractiveShell.separate_out2 = ''

## Show rewritten input, e.g. for autocall.
#  Default: True
# c.InteractiveShell.show_rewritten_input = True

## Enables rich html representation of docstrings. (This requires the docrepr
#  module).
#  Default: False
# c.InteractiveShell.sphinxify_docstring = False

#  Default: True
# c.InteractiveShell.wildcards_case_sensitive = True

## Switch modes for the IPython exception handlers.
#  Choices: any of ['Context', 'Plain', 'Verbose', 'Minimal'] (case-insensitive)
#  Default: 'Context'
# c.InteractiveShell.xmode = 'Context'

#------------------------------------------------------------------------------
# TerminalInteractiveShell(InteractiveShell) configuration
#------------------------------------------------------------------------------
## 
#  See also: InteractiveShell.ast_node_interactivity
# c.TerminalInteractiveShell.ast_node_interactivity = 'last_expr'

## 
#  See also: InteractiveShell.ast_transformers
# c.TerminalInteractiveShell.ast_transformers = []

## Automatically add/delete closing bracket or quote when opening bracket or
#  quote is entered/deleted. Brackets: (), [], {} Quotes: '', ""
#  Default: False
# c.TerminalInteractiveShell.auto_match = False

## 
#  See also: InteractiveShell.autoawait
# c.TerminalInteractiveShell.autoawait = True

## 
#  See also: InteractiveShell.autocall
# c.TerminalInteractiveShell.autocall = 0

## Autoformatter to reformat Terminal code. Can be `'black'`, `'yapf'` or `None`
#  Default: None
# c.TerminalInteractiveShell.autoformatter = None

## 
#  See also: InteractiveShell.autoindent
# c.TerminalInteractiveShell.autoindent = True

## 
#  See also: InteractiveShell.automagic
# c.TerminalInteractiveShell.automagic = True

## Specifies from which source automatic suggestions are provided. Can be set to
#  `'AutoSuggestFromHistory`' or `None` to disableautomatic suggestions. Default
#  is `'AutoSuggestFromHistory`'.
#  Default: 'AutoSuggestFromHistory'
# c.TerminalInteractiveShell.autosuggestions_provider = 'AutoSuggestFromHistory'

## The part of the banner to be printed before the profile
#  See also: InteractiveShell.banner1
# c.TerminalInteractiveShell.banner1 = "Python 3.9.10 (main, Jan 15 2022, 11:48:04) \nType 'copyright', 'credits' or 'license' for more information\nIPython 8.2.0 -- An enhanced Interactive Python. Type '?' for help.\n"

## The part of the banner to be printed after the profile
#  See also: InteractiveShell.banner2
# c.TerminalInteractiveShell.banner2 = ''

## 
#  See also: InteractiveShell.cache_size
# c.TerminalInteractiveShell.cache_size = 1000

## 
#  See also: InteractiveShell.color_info
# c.TerminalInteractiveShell.color_info = True

## Set the color scheme (NoColor, Neutral, Linux, or LightBG).
#  See also: InteractiveShell.colors
# c.TerminalInteractiveShell.colors = 'Neutral'

## Set to confirm when you try to exit IPython with an EOF (Control-D in Unix,
#  Control-Z/Enter in Windows). By typing 'exit' or 'quit', you can force a
#  direct exit without any confirmation.
#  Default: True
# c.TerminalInteractiveShell.confirm_exit = True

#  See also: InteractiveShell.debug
# c.TerminalInteractiveShell.debug = False

## File in which to store and read history
#  Default: '~/.pdbhistory'
# c.TerminalInteractiveShell.debugger_history_file = '~/.pdbhistory'

## Don't call post-execute functions that have failed in the past.
#  See also: InteractiveShell.disable_failing_post_execute
# c.TerminalInteractiveShell.disable_failing_post_execute = False

## Options for displaying tab completions, 'column', 'multicolumn', and
#  'readlinelike'. These options are for `prompt_toolkit`, see `prompt_toolkit`
#  documentation for more information.
#  Choices: any of ['column', 'multicolumn', 'readlinelike']
#  Default: 'multicolumn'
# c.TerminalInteractiveShell.display_completions = 'multicolumn'

## If True, anything that would be passed to the pager
#  See also: InteractiveShell.display_page
# c.TerminalInteractiveShell.display_page = False

## Shortcut style to use at the prompt. 'vi' or 'emacs'.
#  Default: 'emacs'
# c.TerminalInteractiveShell.editing_mode = 'emacs'

## Set the editor used by IPython (default to $EDITOR/vi/notepad).
#  Default: 'vi'
# c.TerminalInteractiveShell.editor = 'vi'

## Add shortcuts from 'emacs' insert mode to 'vi' insert mode.
#  Default: True
# c.TerminalInteractiveShell.emacs_bindings_in_vi_insert_mode = True

## Allows to enable/disable the prompt toolkit history search
#  Default: True
# c.TerminalInteractiveShell.enable_history_search = True

## 
#  See also: InteractiveShell.enable_html_pager
# c.TerminalInteractiveShell.enable_html_pager = False

## Enable vi (v) or Emacs (C-X C-E) shortcuts to open an external editor. This is
#  in addition to the F2 binding, which is always enabled.
#  Default: False
# c.TerminalInteractiveShell.extra_open_editor_shortcuts = False

## Provide an alternative handler to be called when the user presses Return. This
#  is an advanced option intended for debugging, which may be changed or removed
#  in later releases.
#  Default: None
# c.TerminalInteractiveShell.handle_return = None

## Highlight matching brackets.
#  Default: True
# c.TerminalInteractiveShell.highlight_matching_brackets = True

## The name or class of a Pygments style to use for syntax
#          highlighting. To see available styles, run `pygmentize -L styles`.
#  Default: traitlets.Undefined
# c.TerminalInteractiveShell.highlighting_style = traitlets.Undefined

## Override highlighting format for specific tokens
#  Default: {}
# c.TerminalInteractiveShell.highlighting_style_overrides = {}

## Total length of command history
#  See also: InteractiveShell.history_length
# c.TerminalInteractiveShell.history_length = 10000

## 
#  See also: InteractiveShell.history_load_length
# c.TerminalInteractiveShell.history_load_length = 1000

#  See also: InteractiveShell.ipython_dir
# c.TerminalInteractiveShell.ipython_dir = ''

## 
#  See also: InteractiveShell.logappend
# c.TerminalInteractiveShell.logappend = ''

## 
#  See also: InteractiveShell.logfile
# c.TerminalInteractiveShell.logfile = ''

## 
#  See also: InteractiveShell.logstart
# c.TerminalInteractiveShell.logstart = False

## Select the loop runner that will be used to execute top-level asynchronous
#  code
#  See also: InteractiveShell.loop_runner
# c.TerminalInteractiveShell.loop_runner = 'IPython.core.interactiveshell._asyncio_runner'

#  Default: {}
# c.TerminalInteractiveShell.mime_renderers = {}

## Cursor shape changes depending on vi mode: beam in vi insert mode, block in
#  nav mode, underscore in replace mode.
#  Default: True
# c.TerminalInteractiveShell.modal_cursor = True

## Enable mouse support in the prompt (Note: prevents selecting text with the
#  mouse)
#  Default: False
# c.TerminalInteractiveShell.mouse_support = False

#  See also: InteractiveShell.object_info_string_level
# c.TerminalInteractiveShell.object_info_string_level = 0

## 
#  See also: InteractiveShell.pdb
# c.TerminalInteractiveShell.pdb = False

## Display the current vi mode (when using vi editing mode).
#  Default: True
# c.TerminalInteractiveShell.prompt_includes_vi_mode = True

## Class used to generate Prompt token for prompt_toolkit
#  Default: 'IPython.terminal.prompts.Prompts'
# c.TerminalInteractiveShell.prompts_class = 'IPython.terminal.prompts.Prompts'

#  See also: InteractiveShell.quiet
# c.TerminalInteractiveShell.quiet = False

#  See also: InteractiveShell.separate_in
# c.TerminalInteractiveShell.separate_in = '\n'

#  See also: InteractiveShell.separate_out
# c.TerminalInteractiveShell.separate_out = ''

#  See also: InteractiveShell.separate_out2
# c.TerminalInteractiveShell.separate_out2 = ''

## Show rewritten input, e.g. for autocall.
#  See also: InteractiveShell.show_rewritten_input
# c.TerminalInteractiveShell.show_rewritten_input = True

## Use `raw_input` for the REPL, without completion and prompt colors.
#  
#              Useful when controlling IPython as a subprocess, and piping STDIN/OUT/ERR. Known usage are:
#              IPython own testing machinery, and emacs inferior-shell integration through elpy.
#  
#              This mode default to `True` if the `IPY_TEST_SIMPLE_PROMPT`
#              environment variable is set, or the current terminal is not a tty.
#  Default: False
# c.TerminalInteractiveShell.simple_prompt = False

## Number of line at the bottom of the screen to reserve for the tab completion
#  menu, search history, ...etc, the height of these menus will at most this
#  value. Increase it is you prefer long and skinny menus, decrease for short and
#  wide.
#  Default: 6
# c.TerminalInteractiveShell.space_for_menu = 6

## 
#  See also: InteractiveShell.sphinxify_docstring
# c.TerminalInteractiveShell.sphinxify_docstring = False

## Automatically set the terminal title
#  Default: True
# c.TerminalInteractiveShell.term_title = True

## Customize the terminal title format.  This is a python format string.
#  Available substitutions are: {cwd}.
#  Default: 'IPython: {cwd}'
# c.TerminalInteractiveShell.term_title_format = 'IPython: {cwd}'

## The time in milliseconds that is waited for a mapped key
#         sequence to complete.
#  Default: 0.5
# c.TerminalInteractiveShell.timeoutlen = 0.5

## Use 24bit colors instead of 256 colors in prompt highlighting.
#          If your terminal supports true color, the following command should
#          print ``TRUECOLOR`` in orange::
#  
#              printf "\x1b[38;2;255;100;0mTRUECOLOR\x1b[0m\n"
#  Default: False
# c.TerminalInteractiveShell.true_color = False

## The time in milliseconds that is waited for a key code
#         to complete.
#  Default: 0.01
# c.TerminalInteractiveShell.ttimeoutlen = 0.01

#  See also: InteractiveShell.wildcards_case_sensitive
# c.TerminalInteractiveShell.wildcards_case_sensitive = True

## Switch modes for the IPython exception handlers.
#  See also: InteractiveShell.xmode
# c.TerminalInteractiveShell.xmode = 'Context'

#------------------------------------------------------------------------------
# HistoryAccessor(HistoryAccessorBase) configuration
#------------------------------------------------------------------------------
## Access the history database without adding to it.
#  
#      This is intended for use by standalone history tools. IPython shells use
#      HistoryManager, below, which is a subclass of this.

## Options for configuring the SQLite connection
#  
#          These options are passed as keyword args to sqlite3.connect
#          when establishing database connections.
#  Default: {}
# c.HistoryAccessor.connection_options = {}

## enable the SQLite history
#  
#          set enabled=False to disable the SQLite history,
#          in which case there will be no stored history, no SQLite connection,
#          and no background saving thread.  This may be necessary in some
#          threaded environments where IPython is embedded.
#  Default: True
# c.HistoryAccessor.enabled = True

## Path to file to use for SQLite history database.
#  
#          By default, IPython will put the history database in the IPython
#          profile directory.  If you would rather share one history among
#          profiles, you can set this value in each, so that they are consistent.
#  
#          Due to an issue with fcntl, SQLite is known to misbehave on some NFS
#          mounts.  If you see IPython hanging, try setting this to something on a
#          local disk, e.g::
#  
#              ipython --HistoryManager.hist_file=/tmp/ipython_hist.sqlite
#  
#          you can also use the specific value `:memory:` (including the colon
#          at both end but not the back ticks), to avoid creating an history file.
#  Default: traitlets.Undefined
# c.HistoryAccessor.hist_file = traitlets.Undefined

#------------------------------------------------------------------------------
# HistoryManager(HistoryAccessor) configuration
#------------------------------------------------------------------------------
## A class to organize all history-related functionality in one place.

## Options for configuring the SQLite connection
#  See also: HistoryAccessor.connection_options
# c.HistoryManager.connection_options = {}

## Write to database every x commands (higher values save disk access & power).
#  Values of 1 or less effectively disable caching.
#  Default: 0
# c.HistoryManager.db_cache_size = 0

## Should the history database include output? (default: no)
#  Default: False
# c.HistoryManager.db_log_output = False

## enable the SQLite history
#  See also: HistoryAccessor.enabled
# c.HistoryManager.enabled = True

## Path to file to use for SQLite history database.
#  See also: HistoryAccessor.hist_file
# c.HistoryManager.hist_file = traitlets.Undefined

#------------------------------------------------------------------------------
# MagicsManager(Configurable) configuration
#------------------------------------------------------------------------------
## Object that handles all magic-related functionality for IPython.

## Automatically call line magics without requiring explicit % prefix
#  Default: True
# c.MagicsManager.auto_magic = True

## Mapping from magic names to modules to load.
#  
#  This can be used in IPython/IPykernel configuration to declare lazy magics
#  that will only be imported/registered on first use.
#  
#  For example::
#  
#      c.MagicsManager.lazy_magics = {
#        "my_magic": "slow.to.import",
#        "my_other_magic": "also.slow",
#      }
#  
#  On first invocation of `%my_magic`, `%%my_magic`, `%%my_other_magic` or
#  `%%my_other_magic`, the corresponding module will be loaded as an ipython
#  extensions as if you had previously done `%load_ext ipython`.
#  
#  Magics names should be without percent(s) as magics can be both cell and line
#  magics.
#  
#  Lazy loading happen relatively late in execution process, and complex
#  extensions that manipulate Python/IPython internal state or global state might
#  not support lazy loading.
#  Default: {}
# c.MagicsManager.lazy_magics = {}

#------------------------------------------------------------------------------
# ProfileDir(LoggingConfigurable) configuration
#------------------------------------------------------------------------------
## An object to manage the profile directory and its resources.
#  
#      The profile directory is used by all IPython applications, to manage
#      configuration, logging and security.
#  
#      This object knows how to find, create and manage these directories. This
#      should be used by any code that wants to handle profiles.

## Set the profile location directly. This overrides the logic used by the
#          `profile` option.
#  Default: ''
# c.ProfileDir.location = ''

#------------------------------------------------------------------------------
# BaseFormatter(Configurable) configuration
#------------------------------------------------------------------------------
## A base formatter class that is configurable.
#  
#      This formatter should usually be used as the base class of all formatters.
#      It is a traited :class:`Configurable` class and includes an extensible
#      API for users to determine how their objects are formatted. The following
#      logic is used to find a function to format an given object.
#  
#      1. The object is introspected to see if it has a method with the name
#         :attr:`print_method`. If is does, that object is passed to that method
#         for formatting.
#      2. If no print method is found, three internal dictionaries are consulted
#         to find print method: :attr:`singleton_printers`, :attr:`type_printers`
#         and :attr:`deferred_printers`.
#  
#      Users should use these dictionaries to register functions that will be
#      used to compute the format data for their objects (if those objects don't
#      have the special print methods). The easiest way of using these
#      dictionaries is through the :meth:`for_type` and :meth:`for_type_by_name`
#      methods.
#  
#      If no function/callable is found to compute the format data, ``None`` is
#      returned and this format type is not used.

#  Default: {}
# c.BaseFormatter.deferred_printers = {}

#  Default: True
# c.BaseFormatter.enabled = True

#  Default: {}
# c.BaseFormatter.singleton_printers = {}

#  Default: {}
# c.BaseFormatter.type_printers = {}

#------------------------------------------------------------------------------
# PlainTextFormatter(BaseFormatter) configuration
#------------------------------------------------------------------------------
## The default pretty-printer.
#  
#      This uses :mod:`IPython.tapdata_cli.pretty` to compute the format data of
#      the object. If the object cannot be pretty printed, :func:`repr` is used.
#      See the documentation of :mod:`IPython.tapdata_cli.pretty` for details on
#      how to write pretty printers.  Here is a simple example::
#  
#          def dtype_pprinter(obj, p, cycle):
#              if cycle:
#                  return p.text('dtype(...)')
#              if hasattr(obj, 'fields'):
#                  if obj.fields is None:
#                      p.text(repr(obj))
#                  else:
#                      p.begin_group(7, 'dtype([')
#                      for i, field in enumerate(obj.descr):
#                          if i > 0:
#                              p.text(',')
#                              p.breakable()
#                          p.pretty(field)
#                      p.end_group(7, '])')

#  See also: BaseFormatter.deferred_printers
# c.PlainTextFormatter.deferred_printers = {}

#  Default: ''
# c.PlainTextFormatter.float_precision = ''

## Truncate large collections (lists, dicts, tuples, sets) to this size.
#  
#          Set to 0 to disable truncation.
#  Default: 1000
# c.PlainTextFormatter.max_seq_length = 1000

#  Default: 79
# c.PlainTextFormatter.max_width = 79

#  Default: '\n'
# c.PlainTextFormatter.newline = '\n'

#  Default: True
# c.PlainTextFormatter.pprint = True

#  See also: BaseFormatter.singleton_printers
# c.PlainTextFormatter.singleton_printers = {}

#  See also: BaseFormatter.type_printers
# c.PlainTextFormatter.type_printers = {}

#  Default: False
# c.PlainTextFormatter.verbose = False

#------------------------------------------------------------------------------
# Completer(Configurable) configuration
#------------------------------------------------------------------------------
## Enable unicode completions, e.g. \alpha<tab> . Includes completion of latex
#  commands, unicode names, and expanding unicode characters back to latex
#  commands.
#  Default: True
# c.Completer.backslash_combining_completions = True

## Enable debug for the Completer. Mostly print extra information for
#  experimental jedi integration.
#  Default: False
# c.Completer.debug = False

## Activate greedy completion
#          PENDING DEPRECATION. this is now mostly taken care of with Jedi.
#  
#          This will enable completion on elements of lists, results of function calls, etc.,
#          but can be unsafe because the code is actually evaluated on TAB.
#  Default: False
# c.Completer.greedy = False

## Experimental: restrict time (in milliseconds) during which Jedi can compute types.
#          Set to 0 to stop computing types. Non-zero value lower than 100ms may hurt
#          performance by preventing jedi to build its cache.
#  Default: 400
# c.Completer.jedi_compute_type_timeout = 400

## Experimental: Use Jedi to generate autocompletions. Default to True if jedi is
#  installed.
#  Default: True
# c.Completer.use_jedi = True

#------------------------------------------------------------------------------
# IPCompleter(Completer) configuration
#------------------------------------------------------------------------------
## Extension of the completer class with IPython-specific features

## Enable unicode completions, e.g. \alpha<tab> . Includes completion of latex
#  commands, unicode names, and expanding unicode characters back to latex
#  commands.
#  See also: Completer.backslash_combining_completions
# c.IPCompleter.backslash_combining_completions = True

## Enable debug for the Completer. Mostly print extra information for
#  experimental jedi integration.
#  See also: Completer.debug
# c.IPCompleter.debug = False

## Activate greedy completion
#  See also: Completer.greedy
# c.IPCompleter.greedy = False

## Experimental: restrict time (in milliseconds) during which Jedi can compute
#  types.
#  See also: Completer.jedi_compute_type_timeout
# c.IPCompleter.jedi_compute_type_timeout = 400

## DEPRECATED as of version 5.0.
#  
#  Instruct the completer to use __all__ for the completion
#  
#  Specifically, when completing on ``object.<tab>``.
#  
#  When True: only those names in obj.__all__ will be included.
#  
#  When False [default]: the __all__ attribute is ignored
#  Default: False
# c.IPCompleter.limit_to__all__ = False

## Whether to merge completion results into a single list
#  
#          If False, only the completion results from the first non-empty
#          completer will be returned.
#  Default: True
# c.IPCompleter.merge_completions = True

## Instruct the completer to omit private method names
#  
#          Specifically, when completing on ``object.<tab>``.
#  
#          When 2 [default]: all names that start with '_' will be excluded.
#  
#          When 1: all 'magic' names (``__foo__``) will be excluded.
#  
#          When 0: nothing will be excluded.
#  Choices: any of [0, 1, 2]
#  Default: 2
# c.IPCompleter.omit__names = 2

## If True, emit profiling data for completion subsystem using cProfile.
#  Default: False
# c.IPCompleter.profile_completions = False

## Template for path at which to output profile data for completions.
#  Default: '.completion_profiles'
# c.IPCompleter.profiler_output_dir = '.completion_profiles'

## Experimental: Use Jedi to generate autocompletions. Default to True if jedi is
#  installed.
#  See also: Completer.use_jedi
# c.IPCompleter.use_jedi = True

#------------------------------------------------------------------------------
# ScriptMagics(Magics) configuration
#------------------------------------------------------------------------------
## Magics for talking to scripts
#  
#      This defines a base `%%script` cell magic for running a cell
#      with a program in a subprocess, and registers a few top-level
#      magics that call %%script with common interpreters.

## Extra script cell magics to define
#  
#          This generates simple wrappers of `%%script foo` as `%%foo`.
#  
#          If you want to add script magics that aren't on your path,
#          specify them in script_paths
#  Default: []
# c.ScriptMagics.script_magics = []

## Dict mapping short 'ruby' names to full paths, such as '/opt/secret/bin/ruby'
#  
#          Only necessary for items in script_magics where the default path will not
#          find the right interpreter.
#  Default: {}
# c.ScriptMagics.script_paths = {}

#------------------------------------------------------------------------------
# LoggingMagics(Magics) configuration
#------------------------------------------------------------------------------
## Magics related to all logging machinery.

## Suppress output of log state when logging is enabled
#  Default: False
# c.LoggingMagics.quiet = False

#------------------------------------------------------------------------------
# StoreMagics(Magics) configuration
#------------------------------------------------------------------------------
## Lightweight persistence for python variables.
#  
#      Provides the %store magic.

## If True, any %store-d variables will be automatically restored
#          when IPython starts.
#  Default: False
# c.StoreMagics.autorestore = False
from IPython.terminal.prompts import Prompts, Token
import os

class MyPrompt(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [(Token.Prompt, '>>> ')]

    def out_prompt_tokens(self):
       return []

c.TerminalInteractiveShell.prompts_class = MyPrompt
