package com.tapdata.constant;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.Security;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SSLUtilTest {

    String sslCA = "-----BEGIN CERTIFICATE-----\n" +
            "MIIFCzCCAvOgAwIBAgIUNGsTrUxV4D5dmZ15RB6A5ox4xrEwDQYJKoZIhvcNAQEL\n" +
            "BQAwFTETMBEGA1UEAwwKVGVzdFJvb3RDQTAeFw0yNjAzMjUwMjI5NDBaFw0zNjAz\n" +
            "MjIwMjI5NDBaMBUxEzARBgNVBAMMClRlc3RSb290Q0EwggIiMA0GCSqGSIb3DQEB\n" +
            "AQUAA4ICDwAwggIKAoICAQCauNOjoEUC/g75gbgbaOiqnay4NXRAC5STWAASZ2cf\n" +
            "beR38xYsTmWUNzj7WJ/oIj5WiHCJRzkBooGH8WqhkE7juiT65q0hvQlqCJKwkjkG\n" +
            "3v9vTbHpVa9VlKJVyfnfom9IMCD213iKnlIa2pDfV1eeq8zyjD4Isf9ek6DrOdfg\n" +
            "BgkvX0IG4gYBdT9dd4/WfGjanYe9YDWdYYltUHgaM/kfvdJh+bKQ4zDCREmDZWAc\n" +
            "HlZEDndr5bvgmOETTEw5B4/aIUfRqKBudPj0jgR/plKZhxFowjJXXrSuDPoShqKI\n" +
            "M96+fckyOkgdwysYCBqRcuaf2P0L05D7u2gH6bMxIqcIaLJy5Ej8Wn4g7LzPQPyF\n" +
            "TrIDCEdgQhR+9A45o68bIG8Kayx/8qg8eJKV8XqsYTftiP4ZmJMPfcjQz/YBhOnV\n" +
            "EgmNDJYystjJM6YCuMEtbjhR9OwHOm8PbdDUzvx2iU+AECIt0jSMzKrlznkgQ21J\n" +
            "bbm5qzT08C1nFdwX94Q8tXX7jFaeqpYy36SXs2ICUpHK7c9xgzHL9hp/I4jQmWQQ\n" +
            "9JGQ2NqbhFWgl8OFzvdxOlneqAYPJqFIpmF+nCb5NNoWEmFEsYgMby98aFyBmMdh\n" +
            "Le4hNZyXXMk1HrvSM5ZdWkUA7yorpndFRdZM+N1PQeq6Gd69ZJ21WwV6OWGCkE6P\n" +
            "bQIDAQABo1MwUTAdBgNVHQ4EFgQUV/C/CrFAsjYfLhfyV/nyWTgC5fIwHwYDVR0j\n" +
            "BBgwFoAUV/C/CrFAsjYfLhfyV/nyWTgC5fIwDwYDVR0TAQH/BAUwAwEB/zANBgkq\n" +
            "hkiG9w0BAQsFAAOCAgEAZpA4Dgn+2F/brI815upWMrjmZliK/JKVjXAprqwoZ0ce\n" +
            "Vc0RJmDJDPBzM9dyiO3fNvncYNqdvXfo1tYcrX4cmJ5FXyejzyoNZAG+xCzi4X1f\n" +
            "VTPAxkWbwwFaCu5xhUbHkfWAJWIcGW7MXwG+z8QGwGsCT+GflIm7rLc6KTk7Uvie\n" +
            "QSZrY1Ogyv47oxcqW4+vIwODsiI4Znr+TCr1ltGGb1txhZCGz9bO9KSEL3TClkVi\n" +
            "OLSGoFwfH9+n1pAUrDuWdMcpO7EzCNCG8dSr3YnmsoqSuuSs9SvKlma8avUesyJw\n" +
            "0Iu013Yp/NdZYc+fPBup0al3yuBzolR3SZB0iVIWWYiHeWqVL9HczCWeNWtkA0t0\n" +
            "qQmiPcR+GverFdMdjAyaXGK83PriDIKE8XVTvy1izDGt0gS3KxBYmaxxEpKnqeLf\n" +
            "Hf+jGxSQi4Q+E7trGDiLOlXjh0fTxnL3BZ/9p/BDj9alzdlAtUx9eTSl1OTT7EE/\n" +
            "vrg409WvWwqVN3KqhW10GXTvFBCMQv5siTKotcEoWQMTcx6wIs1z/YXmBmucuxYO\n" +
            "aS46rjKkTaITGU/KvCFc+8NQUfk+q3NzyZ1Q9EHrgkLUnPzO9K1XHaUNAQ+pn8dE\n" +
            "mXxLt+3IdFT7HHMfsIlYPVlK4IV8qNLheRlg5bgSzwjRe5FbOmfS8TzBBTpa2Ok=\n" +
            "-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\n" +
            "MIIFCzCCAvOgAwIBAgIUNGsTrUxV4D5dmZ15RB6A5ox4xrEwDQYJKoZIhvcNAQEL\n" +
            "BQAwFTETMBEGA1UEAwwKVGVzdFJvb3RDQTAeFw0yNjAzMjUwMjI5NDBaFw0zNjAz\n" +
            "MjIwMjI5NDBaMBUxEzARBgNVBAMMClRlc3RSb290Q0EwggIiMA0GCSqGSIb3DQEB\n" +
            "AQUAA4ICDwAwggIKAoICAQCauNOjoEUC/g75gbgbaOiqnay4NXRAC5STWAASZ2cf\n" +
            "beR38xYsTmWUNzj7WJ/oIj5WiHCJRzkBooGH8WqhkE7juiT65q0hvQlqCJKwkjkG\n" +
            "3v9vTbHpVa9VlKJVyfnfom9IMCD213iKnlIa2pDfV1eeq8zyjD4Isf9ek6DrOdfg\n" +
            "BgkvX0IG4gYBdT9dd4/WfGjanYe9YDWdYYltUHgaM/kfvdJh+bKQ4zDCREmDZWAc\n" +
            "HlZEDndr5bvgmOETTEw5B4/aIUfRqKBudPj0jgR/plKZhxFowjJXXrSuDPoShqKI\n" +
            "M96+fckyOkgdwysYCBqRcuaf2P0L05D7u2gH6bMxIqcIaLJy5Ej8Wn4g7LzPQPyF\n" +
            "TrIDCEdgQhR+9A45o68bIG8Kayx/8qg8eJKV8XqsYTftiP4ZmJMPfcjQz/YBhOnV\n" +
            "EgmNDJYystjJM6YCuMEtbjhR9OwHOm8PbdDUzvx2iU+AECIt0jSMzKrlznkgQ21J\n" +
            "bbm5qzT08C1nFdwX94Q8tXX7jFaeqpYy36SXs2ICUpHK7c9xgzHL9hp/I4jQmWQQ\n" +
            "9JGQ2NqbhFWgl8OFzvdxOlneqAYPJqFIpmF+nCb5NNoWEmFEsYgMby98aFyBmMdh\n" +
            "Le4hNZyXXMk1HrvSM5ZdWkUA7yorpndFRdZM+N1PQeq6Gd69ZJ21WwV6OWGCkE6P\n" +
            "bQIDAQABo1MwUTAdBgNVHQ4EFgQUV/C/CrFAsjYfLhfyV/nyWTgC5fIwHwYDVR0j\n" +
            "BBgwFoAUV/C/CrFAsjYfLhfyV/nyWTgC5fIwDwYDVR0TAQH/BAUwAwEB/zANBgkq\n" +
            "hkiG9w0BAQsFAAOCAgEAZpA4Dgn+2F/brI815upWMrjmZliK/JKVjXAprqwoZ0ce\n" +
            "Vc0RJmDJDPBzM9dyiO3fNvncYNqdvXfo1tYcrX4cmJ5FXyejzyoNZAG+xCzi4X1f\n" +
            "VTPAxkWbwwFaCu5xhUbHkfWAJWIcGW7MXwG+z8QGwGsCT+GflIm7rLc6KTk7Uvie\n" +
            "QSZrY1Ogyv47oxcqW4+vIwODsiI4Znr+TCr1ltGGb1txhZCGz9bO9KSEL3TClkVi\n" +
            "OLSGoFwfH9+n1pAUrDuWdMcpO7EzCNCG8dSr3YnmsoqSuuSs9SvKlma8avUesyJw\n" +
            "0Iu013Yp/NdZYc+fPBup0al3yuBzolR3SZB0iVIWWYiHeWqVL9HczCWeNWtkA0t0\n" +
            "qQmiPcR+GverFdMdjAyaXGK83PriDIKE8XVTvy1izDGt0gS3KxBYmaxxEpKnqeLf\n" +
            "Hf+jGxSQi4Q+E7trGDiLOlXjh0fTxnL3BZ/9p/BDj9alzdlAtUx9eTSl1OTT7EE/\n" +
            "vrg409WvWwqVN3KqhW10GXTvFBCMQv5siTKotcEoWQMTcx6wIs1z/YXmBmucuxYO\n" +
            "aS46rjKkTaITGU/KvCFc+8NQUfk+q3NzyZ1Q9EHrgkLUnPzO9K1XHaUNAQ+pn8dE\n" +
            "mXxLt+3IdFT7HHMfsIlYPVlK4IV8qNLheRlg5bgSzwjRe5FbOmfS8TzBBTpa2Ok=\n" +
            "-----END CERTIFICATE-----";
    String sslPEM = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
            "MIIJrTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQIKDoWRTG8520CAggA\n" +
            "MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBA2aloSfbqLMoYqldbUfWUUBIIJ\n" +
            "UAUsRotyR7xe3aSLcDG2Jzau1vQQfCJLg8UAX63BYn+yRoIBBMk5nhks+5pD+EQa\n" +
            "pKQ2/soiAUZaJhEXhK3AMaaY5+nYlF3VwlzMYFltgL+N+SxAZAqM984vtvA5+ozv\n" +
            "bGwRHcT8iOWgO0zTNHnufJD9le16pLEJUKbU2RSJnMBCMCd3GxILBZ25FXV9Ej3X\n" +
            "hbY+uKH1s4QdZV3cwYfoPm7Ze9zL6maCTma9Fg5iMACmBnwY68QcL53TRuLFg/z0\n" +
            "Xsz9vF8gW/mCUfMDnyBbQ8bry8zT/3K3SDAmNTk7qPjc2EUnTH26CPSwQ3Glhlg4\n" +
            "/3aljGZ2aU7WjtLhIY4axJtv19qJsf6bZhRGRQ3h3jWo9yZTxj55IfmAtnSqY8Of\n" +
            "vvf7KNW/epEH5DFDIULgv75v03H9qgmxTIy1E38PcxyiEUNY+4ImmJIIbQI4CaLn\n" +
            "K58Lqm0sdfE3I6CUSfYfXhFFdrAyPLGWEYoUrz+I8P2Qfhd8tWbyAv39YEb5iXJJ\n" +
            "9W0LsiUFD9mY2g34WdRdIYvNzUHWF7+GqCLlEXUqKBeIOTIXUUCAoBTvftwQ2llR\n" +
            "doGw7+CmOgN5ladJCLkg+to6Q5ADrfXRAjei0DP4SudXbbEZDpliKftNqQwa4332\n" +
            "jQ9zoZGzw3cOGBM01ABcn3JrGtlpEjlJUDjDvADxo3+mf6DaPryBvzQ9dDQFeP/z\n" +
            "wKleuC1tvAT0NrJdc7VAArH5vw5frvOxBRQpjJnHkbRtPaagSrafemmMskDii7NW\n" +
            "G7YARH7U5ggiZK85lQnP+5kutWcVWSNlvXsEIYZUAywGTvxLOlisszPnteCuikLh\n" +
            "wYaFh/Ty1/EnTy8WcR1H7jU2M03tMt5W/dYlY7LohGlNCJ7nwvKauofAEXhGyw3F\n" +
            "HlGLawcuCeOWlEfyxP1+8F1HM3HT3hAqrJWWH//F7FTkIv2MLBix9iYO+ql/YPoa\n" +
            "TbFZXyyPKsmKlVGy4cKg5vyLuW1G8bEC+V5ziPFW3BbxFWaAOSCr6N6/3pJI1Y+e\n" +
            "0nBVzVJnEypS+3v0sq8/6JXMNnCLTTukBQb7eMyzW6ns1ZrPog09jLT1qJO+DJ0s\n" +
            "nD9ZdOKgbpx8+qjbffAN8L3PBRdAAovsmxU2e0iYkQY+sKf0EfiDvLqMrALjQJLa\n" +
            "kPAQhLR+HXgbim6uawVUoUQ/ylKZJUB8Kuu+NhZaL1NPqEkotaQ/uJPTxdvlYNvv\n" +
            "bd0Gg3ENT17O5eRn7wDpbvF9p4SCGbZdjvVKUvtKyAS0tKlb/i2OqTJ/dyv9jvVq\n" +
            "oHGaQatRi7IwyqLo4vlxfxbMa0+mE1WP2e7u3no7AZIczObS+lfuZoCXZqxy+aBH\n" +
            "iLmm4VudOG/JlAoqIU67JQs8RyPpR7hCy/9EfV4ZT3j4rO5YO/57Hk0gD4/QvUYL\n" +
            "3TNX/l8PRjlIyRUh022nhkMjf3E0ddkM0N4d6Ra7RpB4t7Bfgg3DOF3PY5d2KLHO\n" +
            "rcT2gSHzBwUzzt/fP1Z+QDOEx15+U2KI+JpXi6+EBn3uZfeEnuFXHX2TWBzdPcl/\n" +
            "zv9kteJYZ1btXKGFRhg/Rg9O4vZkSJMGEmOropC8O7tyt3NS1Daaw2Dr/LWPXHyE\n" +
            "lSZBvthGqIJuFoeYLpQe336NGnY9yuD3ah31m6N8rdnzvg9T9A2P1BjITrAqOHu3\n" +
            "f8BTZC7tM+5dLLxW+TYtjKt1cOgZKYV9bfNQOL/b93ngNIlYkEyhSwMOJVfHIpxK\n" +
            "LLJV39PIQxwRRy16DykcpT6eoZJrrJTM6CS26JxfLwuCMaIOsBU+XLQV6v5oW5g4\n" +
            "C7HQPm74ejdcddycX+0iTbMW8otIX0pRYdvAkpA44671qyT+uM9VOjJkfNrbywQR\n" +
            "90LdbWMrU2KCY6f/+yAJwnyp6x6lpKIQQRA9R7IPYYizP7xPjrRHydgFE3y0UeIE\n" +
            "24A5qxxQxK4qCjecZMRwr3ZvK7XvCDNQMZTkeMkESx6wer5Q9oTBU75O1EPcpeMl\n" +
            "R2vBuVVMehihVvFLILTWRn7gPs/63o4/wTglkZA6eBWYRjNTlgaCGVrCMeSXYeQ4\n" +
            "TYBIgA02HEqEGznGJ8spCloOaEMYbT/dlJFu27YH6fnrL+GxuXl26HMTcgsLTbxA\n" +
            "s1Lob8O2LlCafL6wX3kUKmAJQX6MxAZDsnDyV2TR+qIMKti9qBKgGGN9sLnGeZVq\n" +
            "Ob4lKX+k+cR7PPZz/mgNcIxhUno4IyXZ2zNZccI0ZP/4Kry5ibZduv/dKUYZn7tu\n" +
            "6h/43uwOM0AXjIoP1lInx3LSW6yRFFToqwG1mzgxZSIQ4pZhMfcr9va2azQ2yNy+\n" +
            "d4TZCneu9x+gFjr0VVTlvOdlxcM+SeW4aifwyqbPwZy8pFbvrx8/spytQ9AphBer\n" +
            "dVSZiqATeDP4UcMp7EFJ7qL+RGy8APPSu9pgiq4G+Dms1scwAVlg+dwrpWNLSpMV\n" +
            "GUNCXf3JUq7bdRgNsx3pi+t988KWt6t9DOV9Edm95xVY542lZ+O3M92wI2PM9yhM\n" +
            "gdkOl/7lQXiMDqTEl0er4UJQSBraWitEpJvPZThZOWtxGAw0iAmqFMTAP1uCJkaj\n" +
            "GyoONZqbeM5FiA79CSPsZk3ws3U+vILqaraOjG2jwIxzdFjR48owPYONytSCs6dc\n" +
            "joEgwBGjVvwAnF1m1hCq1aqNUv8ZZ8rgzMDAEkRXCfOAn6puPbpFMhs3CTiZvk8j\n" +
            "la0nQ6DtXJ+rTjhDCrfeYkUXNgmc1Gtf4sDN1C2jDRf1XJ6FWrysRHjdWFm0+Ud8\n" +
            "Ypi6Y7E6+XWxb9lrm897CFbO3yWzv7AutUkxXv90E/OvDKCfwEbevZFqW68u+gT0\n" +
            "uTzKWA89tVRHuBrGTQ2n87s03igi5e09Bwazc06BvmJwETabj+zJMFAhYXkqQUEt\n" +
            "4wyFndXg6E+YRoy/0IVebJpjC7KEInyZFzWyevQq2dPw9q3MpPIQYlrFN/1bhStV\n" +
            "O/seObDTJOdqp5hkQvOKA8i7vHtc2X07OJxNXJ7VysSUliRJHy48zOt6dDuCN2Z+\n" +
            "Sz+efTc5X9dS+zdY/0Ri502JJO8AZPxpIh1v+pBBhPJKv4YY6rKsaAsnDXmXTnTZ\n" +
            "hRs8gFg4rKN0alFyXgdpo+tVK11cArR/JyTpzxuZHyLkKW35Apbzmr2DRNZx0fi7\n" +
            "zv8zZednix+jAyaDnO0TayUAEA386wMrs9OC4A3GDXFX\n" +
            "-----END ENCRYPTED PRIVATE KEY-----\n" +
            "-----BEGIN CERTIFICATE-----\n" +
            "MIIFETCCAvmgAwIBAgIUUKkT+F1I0dF2BNn9o489Gm8l/LUwDQYJKoZIhvcNAQEL\n" +
            "BQAwFTETMBEGA1UEAwwKVGVzdFJvb3RDQTAeFw0yNjAzMjUwMjMxNTNaFw0yNzAz\n" +
            "MjUwMjMxNTNaMDIxMDAuBgNVBAMMJ21vbmdvZGItc3NsLm1heGltdXMuaW50ZXJu\n" +
            "YWwudGFwZGF0YS5pbzCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAOHi\n" +
            "OGxGjX2Ymh2rGA6rEXaL3TkC7+JpoVNjLCN6xlyCPGkc8WpTno6kTFEREZdAEpl2\n" +
            "yt6bPX0TTJncRSokdCkYFCSs8fIM1NdKXzubEKJ/Kxdf+g3fdM0TCIPSZanFxKJr\n" +
            "l51bVkJKczgo9/oDdtihpcIM8QSZA1aiWzoXJwjjwSYhnuk5CwTyyun5H9ggs+fX\n" +
            "ekXXAlYWsG6MJUCcqykf4faCzg3Vvag/+S+qW8G5xo7ysPyPQftBb3nZH78b0BRc\n" +
            "+Irq9T9JTcbHEsA7zwbyyqzyFmltdBVljjcnRcA6+YUxFxbkhOLC2i8swnvU2Jce\n" +
            "6d3bjoQLa8MxtPZbp3+ct33s1c2lsqdWYO6WRxIDTFLtOvWGZcx4qHqr2lVSwaVh\n" +
            "Gx87odSY7GXvRYun5sxqI/FughWECitmEvMPAllAXhCiN+DJVsq1tudb53f+p5nX\n" +
            "9jWJoX7/pb3T9p31ZGIA8y+2Xr5nfRMgkDJ7wupwqyt+1PznKQQtj9L/swtXupeH\n" +
            "GQ3oPc0ngg2GtGe4qhPavkGazOGeSllZguI/fBZebWGhw88rcZCfgDSIn2qc8izp\n" +
            "4NCYUVGmyJxxBVBsfTOMXHxux8ygBMekr0cg47mWhLQpuQc+nRdVA/fT/Te18w6K\n" +
            "z3qspC8z20g3w4NO9ic3ssQ/JxT3lNMctY79YzIxAgMBAAGjPDA6MDgGA1UdEQQx\n" +
            "MC+CJ21vbmdvZGItc3NsLm1heGltdXMuaW50ZXJuYWwudGFwZGF0YS5pb4cEwKgB\n" +
            "uDANBgkqhkiG9w0BAQsFAAOCAgEAjFxERRcrOfdi0Q5uiaAJa91UCVtwESderFDv\n" +
            "Ne3m+NSt3/dfB8kCvQc4KshE4n2O0AwWfz648eqsqiMHVh8yDHMnA4PeHbab+EJr\n" +
            "2oGAzq8yZ8/2tykZdSOavN/cFws91vP09lAq237z5HVZyTagwhT3KUPNx5Gu+FYM\n" +
            "1BdpeXrlhzVjnA/mvwyFaGs2c8LdywmBPJy5/ksoMvkvbJmbfjKsA76DYRP68z06\n" +
            "94+kOHWXc/r3y9Kak3UvTOe+3TNdpFa2RoYBT7npsW4BjoJi91946jeoE4DqLaNs\n" +
            "mfrkV6Cx/OuLMC9k6OAuXZPUfuwB5zpcRDOrUqTuQ+LyeJL6F4rUUWp0otzZsvnP\n" +
            "bbd297f9Z1GqDR1TKSWDsgdPoXo/EJZW8LIrK2FwoOCH722yfdBKG1hCfiXTV1Ir\n" +
            "kJxrv6cHKurQtsWDm74IwjwrXCVeKiKNXS/Pcf5pYGwTKqasC7JXlxXLtAyfOIqV\n" +
            "ZWXbthO3cPkXeg1b8VpnN+Ir614ckjhn2xAdgZon9RqF3NBoL+GsCdX3SIB1wf1N\n" +
            "4VoBJg5hXUvpHmCCz+TtXaW64YrbWx7L2AZcEErFmCPE2sNhQQpnYcMFflRps0f/\n" +
            "Z2KMPAo7qs84+F8wjb0yfHcWBRFP0mgFdRdh7CMWmWrbl8tULx9RIpEIAcCbP/RV\n" +
            "GbkjC4Q=\n" +
            "-----END CERTIFICATE-----";

    @BeforeAll
    public static void setup() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void testPrivateKeyWithPassword() {
        String sslPass = "Gotapd8";
        List<String> trustCertificates = SSLUtil.retriveCertificates(sslCA);
        String privateKey = SSLUtil.retrivePrivateKey(sslPEM);
        List<String> certificates = SSLUtil.retriveCertificates(sslPEM);
        assertDoesNotThrow(()->SSLUtil.createSSLContext(privateKey, certificates, trustCertificates, sslPass));
    }

    @Test
    public void testPrivateKeyWithWrongPassword() throws Exception {
        String sslPass = "123456";
        List<String> trustCertificates = SSLUtil.retriveCertificates(sslCA);
        String privateKey = SSLUtil.retrivePrivateKey(sslPEM);
        List<String> certificates = SSLUtil.retriveCertificates(sslPEM);
        Exception exception = assertThrows(Exception.class, () -> SSLUtil.createSSLContext(privateKey, certificates, trustCertificates, sslPass));
        assertTrue(exception.getMessage().contains("Failed to decrypt private key"));
    }

    @Test
    public void testPrivateKeyWithNullPassword() throws Exception {
        String sslPass = null;
        List<String> trustCertificates = SSLUtil.retriveCertificates(sslCA);
        String privateKey = SSLUtil.retrivePrivateKey(sslPEM);
        List<String> certificates = SSLUtil.retriveCertificates(sslPEM);
        Exception exception = assertThrows(Exception.class, () -> SSLUtil.createSSLContext(privateKey, certificates, trustCertificates, sslPass));
        assertTrue(exception.getMessage().contains("DerValue.getBigIntegerInternal, not expected 48"));
    }
}
