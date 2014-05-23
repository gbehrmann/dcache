/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dmg.cells.nucleus;

public abstract class Releases
{
    public static final short RELEASE_2_10 = 0x020A;
    public static final short RELEASE_2_13 = 0x020D;

    public static short getRelease(String version)
    {
        String[] elements = version.replace("-*", "").split("\\.");
        switch (elements.length) {
        case 0:
            throw new NumberFormatException("Invalid dCache version: " + version);
        case 1:
            return (short) (Short.parseShort(elements[0]) << 8);
        default:
            return (short) ((Short.parseShort(elements[0]) << 8) | Short.parseShort(elements[1]));
        }
    }
}
