package experimento;

import java.io.*;
import java.nio.file.*;
import java.util.Random;

public class GeradorArquivos {
    public static void main(String[] args) throws IOException {
        Files.createDirectories(Path.of("arquivos"));

        int[] tamanhosEmMb = {5, 50, 500};
        byte[] bloco = new byte[1024 * 1024];
        new Random(42).nextBytes(bloco);            // arquivos idênticos em toda geração

        for (int mb : tamanhosEmMb) {
            Path p = Path.of("arquivos", "arquivo_" + mb + "MB.bin");
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(p))) {
                for (int i = 0; i < mb; i++) {
                    out.write(bloco);
                }
            }

            System.out.println("Gerado: " + p + " (" + Files.size(p) + " bytes)");
        }


    }
}