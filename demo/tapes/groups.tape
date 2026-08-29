Output docs/gifs/groups.gif

Set Shell        "zsh"
Set FontSize     14
Set Width        1200
Set Height       700
Set TypingSpeed  60ms
Set Theme        "Catppuccin Mocha"

Type "go run ./cmd/readpanda --brokers localhost:19092 --sr-url http://localhost:18081"
Enter
Sleep 2.5s

Type ":"
Sleep 200ms
Type "groups"
Enter
Sleep 4s

Enter
Sleep 3s

Type "q"
Sleep 600ms
