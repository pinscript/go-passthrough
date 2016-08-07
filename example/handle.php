<?php
$data = file_get_contents("php://stdin");
file_put_contents(rand() . "test.txt", $data);