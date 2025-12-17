'To use this macro to calculate column widths, follow these steps:
' 1. Open the Excel file in which you want to calculate column widths.
' 2. Press Alt + F11 to open the VBA editor.
' 3. In the VBA editor, insert a new module by clicking Insert > Module.
' 4. Copy and paste the code into the module.
' 5. Select the tab on which you want to capture column widths.
' 6. Select columns with data and do Format | AutoFit Column Width.
' 7. Run the macro by pressing F5 or by clicking Run > Run Sub/UserForm.
' 8. The macro will display a message box with the column widths dictionary.
' 9. Copy the dictionary from the message box and paste it into your Python code.

Sub GenerateColumnWidthsDictionary()
    Dim ws As Worksheet
    Dim lastCol As Long
    Dim col As Long
    Dim colLetter As String
    Dim colWidth As Double
    Dim roundedWidth As Long
    Dim outputText As String
    Dim lineLength As Long
    Dim maxLineLength As Long
    
    ' Set the active worksheet
    Set ws = ActiveSheet
    
    ' Find the last column with data
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    
    ' Initialize output
    outputText = "column_widths = {" & vbCrLf
    maxLineLength = 120 ' Maximum characters per line for readability
    lineLength = 4 ' Start with 4 spaces indentation
    
    ' Iterate through columns
    For col = 1 To lastCol
        ' Get column letter
        colLetter = Split(ws.Cells(1, col).Address, "$")(1)
        
        ' Get column width, round up, and add 1
        colWidth = ws.Columns(col).ColumnWidth
        roundedWidth = Application.WorksheetFunction.RoundUp(colWidth, 0) + 1
        
        ' Format the entry
        Dim entry As String
        entry = "'" & colLetter & "': " & roundedWidth
        
        ' Add comma if not the last column
        If col < lastCol Then
            entry = entry & ", "
        End If
        
        ' Check if adding this entry would exceed line length
        If lineLength + Len(entry) > maxLineLength And col > 1 Then
            ' Start a new line
            outputText = outputText & vbCrLf & "        " ' 8 spaces for continuation
            lineLength = 8
        End If
        
        ' Add the entry
        outputText = outputText & entry
        lineLength = lineLength + Len(entry)
    Next col
    
    ' Close the dictionary
    outputText = outputText & vbCrLf & "    }"
    
    ' Display the result in a message box that allows copying
    MsgBox outputText, vbInformation, "Column Widths Dictionary - Copy from here"
End Sub

' Helper function to convert column number to letter (for columns beyond Z)
Function ColumnNumberToLetter(colNum As Long) As String
    Dim result As String
    Dim temp As Long
    
    Do While colNum > 0
        temp = (colNum - 1) Mod 26
        result = Chr(65 + temp) & result
        colNum = (colNum - temp - 1) \ 26
    Loop
    
    ColumnNumberToLetter = result
End Function
