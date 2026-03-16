# tablespec screencast — recorded with charmbracelet/vhs
#
# Run:  vhs examples/demo.tape
# Also: asciinema rec -c "bash examples/screencast.sh" tablespec-demo.cast
#
# Both tools record the same screencast.sh script.

Output examples/tablespec-demo.gif
Output examples/tablespec-demo.mp4

Set Shell bash
Set FontFamily "JetBrainsMono Nerd Font Mono"
Set FontSize 14
Set Width 1400
Set Height 900
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 25ms
Set PlaybackSpeed 1

Hide
Type "clear"
Enter
Show
Sleep 500ms

Type "bash examples/screencast.sh"
Enter
Sleep 180s
