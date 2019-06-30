import numpy as np
a = np.around([0.12916282, -0.49347737, -0.07574826, 0.28491855,  0.15095997, 0.45408237], decimals=1)

a[a < 0 ] = 0

print(a)
