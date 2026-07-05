<?php

declare(strict_types=1);

/**
 * How to use skaidb from PHP — modeled on PDO.
 *
 *     php basic_usage.php [host] [port] [user] [password]
 *
 * Uses the driver at ../../drivers/php/src. In a real project, install it
 * via Composer instead of the require below, which exists only so this
 * example runs straight out of the repo.
 */

require __DIR__ . '/../../drivers/php/src/Skaidb.php';

use Skaidb\Connection;
use Skaidb\SkaidbException;

$host = $argv[1] ?? 'localhost';
$port = isset($argv[2]) ? (int) $argv[2] : 7000;
$user = $argv[3] ?? 'anonymous';
$password = $argv[4] ?? '';

try {
    $db = new Connection($host, $port, $user, $password, 'QUORUM');

    // --- DDL ---
    try {
        $db->exec('DROP TABLE people');
    } catch (SkaidbException $e) {
        // table didn't exist — fine
    }
    $db->exec('CREATE TABLE people (PRIMARY KEY (id))');

    // --- Batch insert with a prepared statement (`?` placeholders) ---
    $insert = $db->prepare('INSERT INTO people (id, name, age) VALUES (?, ?, ?)');
    foreach ([[1, 'Ada', 36], [2, 'Linus', 54], [3, 'Margaret', 80]] as $row) {
        $insert->execute($row);
    }

    // --- Query ---
    $stmt = $db->prepare('SELECT id, name, age FROM people WHERE age > ? ORDER BY id');
    $stmt->execute([40]);
    echo "age > 40:\n";
    foreach ($stmt->fetchAll() as $person) {
        printf("  %d %s %d\n", $person['id'], $person['name'], $person['age']);
    }

    // --- Update ---
    $upd = $db->prepare('UPDATE people SET age = ? WHERE id = ?');
    $upd->execute([37, 1]);
    echo 'updated ' . $upd->rowCount() . " row(s)\n";

    // --- Point read by primary key ---
    $one = $db->prepare('SELECT name, age FROM people WHERE id = ?');
    $one->execute([1]);
    $row = $one->fetch();
    echo "id=1: {$row['name']} {$row['age']}\n";

    // --- Error handling ---
    try {
        $db->exec('SELECT * FROM does_not_exist');
    } catch (SkaidbException $e) {
        echo 'expected error: ' . $e->getMessage() . "\n";
    }

    // --- Delete + cleanup ---
    $del = $db->prepare('DELETE FROM people WHERE id = ?');
    $del->execute([2]);
    echo 'deleted ' . $del->rowCount() . " row(s)\n";
    $db->exec('DROP TABLE people');

    $db->close();
} catch (SkaidbException $e) {
    fwrite(STDERR, 'skaidb error: ' . $e->getMessage() . "\n");
    exit(1);
}
