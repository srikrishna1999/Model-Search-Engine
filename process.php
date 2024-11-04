<?php
if ($_SERVER["REQUEST_METHOD"] == "POST") {
    $input = escapeshellarg($_POST["input"]);
    
    $output = shell_exec("./query " . $input);
    
    $html_part1 = '
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Result</title>
        <style>
            table {
                margin: auto;
                border: 1px solid black;
                border-collapse: collapse;
            }
            th, td {
                padding: 8px;
                border: 1px solid black;
                text-align: center;
            }
        </style>
    </head>
    <body>
        <table border-style="dotted">
            <tr>
                <th>Document ID</th>
                <th>Score</th>
            </tr>';

    $html_part2 = '
        </table>
    </body>
    </html>';

    echo $html_part1 . $output . $html_part2;
} else {
    echo "Invalid request.";
}
?>
