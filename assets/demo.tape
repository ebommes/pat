Output assets/demo.gif

Set Shell "bash"
Set FontFamily "ComicShannsMono Nerd Font"
Set FontSize 16
Set Width 900
Set Height 500
Set Theme "Catppuccin Latte"
Set Padding 10
Set WindowBar Colorful

Set TypingSpeed 40ms

Type "# CSV output (default)"
Enter
Sleep 500ms

Type "pat assets/sample.parquet"
Enter
Sleep 2s

Type ""
Enter

Type "# Pretty table output"
Enter
Sleep 500ms

Type "pat --pretty assets/sample.parquet"
Enter
Sleep 2s

Type ""
Enter

Type "# JSON output (jq-compatible)"
Enter
Sleep 500ms

Type "pat --json assets/sample.parquet | jq ."
Enter
Sleep 2s

Type ""
Enter

Type "# Limit to first 3 rows"
Enter
Sleep 500ms

Type "pat -n 3 --pretty assets/sample.parquet"
Enter
Sleep 3s
